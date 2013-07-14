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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import cascading.lingual.catalog.CatalogOptions;
import cascading.lingual.catalog.Repo;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.SchemaDef;
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
import org.apache.ivy.plugins.resolver.URLResolver;
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
  private static final String M2_PER_MODULE_PATTERN = "[revision]/[artifact]-[revision](-[classifier]).[ext]";
  private static final String M2_PATTERN = "[organisation]/[module]/" + M2_PER_MODULE_PATTERN;


  public ProviderTarget( Printer printer, CatalogOptions options )
    {
    super( printer, options );
    }

  @Override
  protected List<String> performAdd( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    SchemaDef schemaDef = catalog.getSchemaDef( getOptions().getSchemaName() );

    String providerName = getOptions().getProviderName();
    File jarFile = getLocalJarFile( platformBroker );

    platformBroker.retrieveInstallProvider( jarFile.getPath() );
    String md5Hash = Misc.getHash( jarFile );

    List<String> names = new ArrayList<String>();
    Properties providerProperties = getProviderProperties( jarFile, true );

    ProviderDefinition[] providerDefinitions = ProviderDefinition.getProviderDefinitions( providerProperties );

    for( ProviderDefinition providerDefinition : providerDefinitions )
      {
      if( providerName != null && !providerDefinition.getProviderName().equals( providerName ) )
        continue;

      if( !providerDefinition.getPlatforms().contains( platformBroker.getName() ) )
        continue;

      String name = providerDefinition.getProviderName();
      Map<String, String> map = providerDefinition.getProviderPropertyMap();

      names.add( name );
      schemaDef.addProviderDef( name, jarFile.getName(), map, md5Hash );
      }

    return names;
    }

  @Override
  protected boolean performRename( PlatformBroker platformBroker )
    {
    return false;
    }

  @Override
  protected boolean performRemove( PlatformBroker platformBroker )
    {
    String providerName = getOptions().getProviderName();

    if( providerName == null )
      throw new IllegalArgumentException( "provider remove action must have a name" );

    SchemaCatalog catalog = platformBroker.getCatalog();
    String schemaName = getOptions().getSchemaName();

    return catalog.removeProviderDef( schemaName, providerName );
    }

  @Override
  protected boolean performValidateDependencies( PlatformBroker platformBroker )
    {
    File jarFile = getLocalJarFile( platformBroker );

    if( !jarFile.exists() && !jarFile.canRead() )
      {
      getPrinter().print( "cannot read from file: " + jarFile.getName() );
      return false;
      }

    getProviderProperties( jarFile, true );

    return true;
    }

  @Override
  protected Collection<String> performGetNames( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    String schemaName = getOptions().getSchemaName();

    return catalog.getProviderNames( schemaName );
    }

  protected File getLocalJarFile( PlatformBroker platformBroker )
    {
    String jarOrSpec = getOptions().getAddURI();

    if( jarOrSpec == null )
      throw new IllegalArgumentException( "either jar uri or maven spec is required to define a provider" );

    if( !jarOrSpec.endsWith( ".jar" ) )
      return retrieveSpec( platformBroker, jarOrSpec );

    URI uri = URI.create( jarOrSpec );

    String scheme = uri.getScheme();
    if( scheme != null && !scheme.startsWith( "file" ) ) // todo: support http
      throw new IllegalArgumentException( "only file or maven specs are supported, got: " + uri );

    File jarFile = new File( jarOrSpec );

    if( jarFile.exists() && jarFile.canRead() )
      return jarFile;

    getPrinter().print( "cannot read from file: " + jarOrSpec );

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
    DefaultDependencyDescriptor dd = new DefaultDependencyDescriptor( md, newInstance( dep[ 0 ], dep[ 1 ], dep[ 2 ] ), false, false, false );
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
      {
      String repoUrl = repo.getRepoUrl();

      if( !repoUrl.endsWith( "/" ) )
        repoUrl += "/";

      RepositoryResolver resolver = new URLResolver();

      if( URI.create( repoUrl ).getScheme() == null )
        repoUrl = new File( repoUrl ).getAbsoluteFile().toURI().toASCIIString();

      resolver.setM2compatible( repo.getRepoKind() == Repo.Kind.Maven2 );
      resolver.setName( repo.getRepoName() );
      resolver.addArtifactPattern( repoUrl + M2_PATTERN );

      resolvers.add( resolver );
      }

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