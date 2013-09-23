/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.lingual.catalog.target;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import cascading.lingual.catalog.CatalogOptions;
import cascading.lingual.catalog.ProviderDef;
import cascading.lingual.catalog.Repo;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.SchemaDef;
import cascading.lingual.catalog.builder.ProviderBuilder;
import cascading.lingual.catalog.provider.ProviderDefinition;
import cascading.lingual.common.Printer;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.util.Misc;
import org.apache.ivy.Ivy;
import org.apache.ivy.core.module.descriptor.DefaultDependencyDescriptor;
import org.apache.ivy.core.module.descriptor.DefaultModuleDescriptor;
import org.apache.ivy.core.report.ResolveReport;
import org.apache.ivy.core.resolve.ResolveOptions;
import org.apache.ivy.core.settings.IvySettings;
import org.apache.ivy.plugins.parser.xml.XmlModuleDescriptorWriter;
import org.apache.ivy.plugins.resolver.ChainResolver;
import org.apache.ivy.plugins.resolver.RepositoryResolver;
import org.apache.ivy.util.DefaultMessageLogger;
import org.apache.ivy.util.Message;

import static cascading.lingual.catalog.provider.CatalogProviderUtil.getProviderProperties;
import static org.apache.ivy.core.module.descriptor.DefaultModuleDescriptor.newDefaultInstance;
import static org.apache.ivy.core.module.id.ModuleRevisionId.newInstance;

/**
 *
 */
public class ProviderTarget extends CRUDTarget
  {
  public ProviderTarget( Printer printer, CatalogOptions options )
    {
    super( printer, options );
    }

  @Override
  protected void validateAdd( PlatformBroker platformBroker )
    {
    doAdd( platformBroker, false );
    }

  @Override
  protected List<String> performAdd( PlatformBroker platformBroker )
    {
    return doAdd( platformBroker, true );
    }

  @Override
  protected boolean performRename( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    String schemaName = getOptions().getSchemaName();
    return catalog.renameProviderDef( schemaName, getOptions().getProviderName(), getOptions().getRenameName() );
    }

  @Override
  protected boolean performRemove( PlatformBroker platformBroker )
    {
    String providerName = getOptions().getProviderName();

    if( providerName == null )
      throw new IllegalArgumentException( "name of provider to remove must be given" );

    SchemaCatalog catalog = platformBroker.getCatalog();
    String schemaName = getOptions().getSchemaName();

    return catalog.removeProviderDef( schemaName, providerName );
    }

  @Override
  protected Object getSource( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    SchemaDef schemaDef = catalog.getSchemaDef( getOptions().getSchemaName() );

    if( schemaDef == null )
      return null;

    return catalog.getSchemaDef( getOptions().getSchemaName() ).getProviderDef( getOptions().getProviderName() );
    }

  @Override
  protected String getRequestedSourceName()
    {
    return getOptions().getProviderName();
    }

  @Override
  protected boolean performValidateDependencies( PlatformBroker platformBroker )
    {
    doAdd( platformBroker, false );
    return true; // doAdd() would have thrown a descriptive Exception if the processing failed.
    }

  @Override
  protected Collection<String> performGetNames( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    String schemaName = getOptions().getSchemaName();

    return catalog.getProviderNames( schemaName );
    }

  @Override
  protected Map performShow( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    SchemaDef schemaDef = catalog.getSchemaDefChecked( getOptions().getSchemaName() );

    ProviderDef providerDef = schemaDef.findProviderDefFor( getOptions().getProviderName() );

    if( providerDef == null )
      return null;

    return new ProviderBuilder().format( providerDef );
    }

  protected List<String> doAdd( PlatformBroker platformBroker, boolean doActualInstall )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    SchemaDef schemaDef = catalog.getSchemaDef( getOptions().getSchemaName() );

    String providerName = getOptions().getProviderName();
    File jarFile = getLocalJarFile( platformBroker );

    platformBroker.retrieveInstallProvider( jarFile.getPath() );
    String md5Hash = Misc.getHash( jarFile );

    List<String> names = new ArrayList<String>();
    Properties providerProperties = getProviderProperties( jarFile );

    ProviderDefinition[] providerDefinitions = ProviderDefinition.getProviderDefinitions( providerProperties );

    if( providerDefinitions.length == 0 )
      throw new IllegalArgumentException( "no provider definition supplied" );

    for( ProviderDefinition providerDefinition : providerDefinitions )
      {
      if( providerName != null && !providerDefinition.getProviderName().equals( providerName ) )
        continue;

      if( !providerDefinition.getPlatforms().contains( platformBroker.getName() ) )
        continue;

      String name = providerDefinition.getProviderName();
      Map<String, String> propertyMap = providerDefinition.getProviderPropertyMap();

      names.add( name );

      if( doActualInstall )
        schemaDef.addProviderDef( name, jarFile.getName(), propertyMap, md5Hash );
      }

    if( names.size() == 0 )
      throw new IllegalArgumentException( "supplied provider definitions not appropriate to platform: " + platformBroker.getName() );

    return names;
    }

  protected File getLocalJarFile( PlatformBroker platformBroker )
    {
    String addJarOrSpec = getOptions().getAddURI();
    String updateJarOrSpec = getOptions().getUpdateURI();

    if( ( addJarOrSpec == null && updateJarOrSpec == null ) || ( addJarOrSpec != null && updateJarOrSpec != null ) )
      throw new IllegalArgumentException( "either jar uri or maven spec is required to define a provider" );

    String jarOrSpec = addJarOrSpec != null ? addJarOrSpec : updateJarOrSpec;

    if( !jarOrSpec.endsWith( ".jar" ) )
      return retrieveSpec( platformBroker, jarOrSpec );

    URI uri = URI.create( jarOrSpec );

    String scheme = uri.getScheme();
    if( scheme != null && !scheme.startsWith( "file" ) ) // todo: support http
      throw new IllegalArgumentException( "only file or maven specs are supported, got: " + uri );

    File jarFile = new File( jarOrSpec );

    if( jarFile.exists() && jarFile.canRead() )
      return jarFile;

    getPrinter().printFormatted( "cannot read from file: " + jarOrSpec );

    throw new IllegalArgumentException( "source jar not valid: " + jarFile.getAbsoluteFile() );
    }

  private File retrieveSpec( PlatformBroker platformBroker, String jarOrSpec )
    {
    IvySettings ivySettings = new IvySettings();

    List<RepositoryResolver> resolvers = getResolvers( platformBroker );

    ChainResolver chainResolver = new ChainResolver();

    for( RepositoryResolver resolver : resolvers )
      chainResolver.add( resolver );

    chainResolver.setName( "chain" );

    ivySettings.addResolver( chainResolver );

    ivySettings.setDefaultResolver( chainResolver.getName() );

    Ivy ivy = Ivy.newInstance( ivySettings );

    String[] dep = jarOrSpec.split( ":" );

    DefaultModuleDescriptor md = newDefaultInstance( newInstance( dep[ 0 ], dep[ 1 ] + "-caller", "working" ) );

    md.addExtraAttributeNamespace( "m", Ivy.getIvyHomeURL() + "maven" );

    Map attributes = new HashMap();

    if( dep.length == 4 )
      attributes.put( "m:classifier", dep[ 3 ] );

    DefaultDependencyDescriptor dd = new DefaultDependencyDescriptor( md, newInstance( dep[ 0 ], dep[ 1 ], dep[ 2 ], attributes ), false, false, false );

    md.addDependency( dd );

    ResolveOptions resolveOptions = new ResolveOptions().setConfs( new String[]{"default"} ).setTransitive( false );

    Message.setDefaultLogger( new DefaultMessageLogger( Message.MSG_VERBOSE ) );

    ResolveReport report = createResolveReport( ivy, md, resolveOptions );

    return report.getAllArtifactsReports()[ 0 ].getLocalFile();
    }

  private ResolveReport createResolveReport( Ivy ivy, DefaultModuleDescriptor md, ResolveOptions resolveOptions )
    {
    File ivyFile = createTempFile();

    try
      {
      XmlModuleDescriptorWriter.write( md, ivyFile );
      return ivy.resolve( ivyFile.toURI().toURL(), resolveOptions );
      }
    catch( Exception exception )
      {
      throw new RuntimeException( "unable to create ivy settings", exception );
      }
    }

  private List<RepositoryResolver> getResolvers( PlatformBroker platformBroker )
    {
    Collection<Repo> repositories = platformBroker.getCatalog().getRepositories();
    List<RepositoryResolver> resolvers = new ArrayList<RepositoryResolver>();

    for( Repo repo : repositories )
      resolvers.add( RepoTarget.getRepositoryResolver( repo ) );

    return resolvers;
    }

  private File createTempFile()
    {
    File file;

    try
      {
      file = File.createTempFile( "ivy", ".xml" );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "could not create temp file", exception );
      }

    file.deleteOnExit();

    return file;
    }
  }