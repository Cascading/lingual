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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import cascading.lingual.catalog.CatalogOptions;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.SchemaDef;
import cascading.lingual.catalog.provider.ProviderDefinition;
import cascading.lingual.common.Printer;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.util.Misc;

import static cascading.lingual.catalog.provider.CatalogProviderUtil.getProviderProperties;

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
  protected List<String> performAdd( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    SchemaDef schemaDef = catalog.getSchemaDef( getOptions().getSchemaName() );

    String providerName = getOptions().getProviderName();
    File jarFile = getJarFile(); // todo: use uri instead of File for remote jars

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
    File jarFile = getJarFile();

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

  protected File getJarFile()
    {
    String jarOrSpec = getOptions().getAddURI();

    if( jarOrSpec == null )
      throw new IllegalArgumentException( "either jar uri or maven spec is required to define a provider" );

    if( !jarOrSpec.endsWith( ".jar" ) )
      throw new UnsupportedOperationException( "spec is currently not supported" );

    File jarFile = new File( jarOrSpec );

    if( jarFile.exists() && jarFile.canRead() )
      return jarFile;

    getPrinter().print( "cannot read from file: " + jarOrSpec );

    throw new IllegalArgumentException( "source jar not valid: " + jarFile.getAbsoluteFile() );
    }
  }
