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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import cascading.lingual.catalog.CatalogOptions;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.ProviderDef;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.SchemaDef;
import cascading.lingual.catalog.builder.FormatBuilder;
import cascading.lingual.common.Printer;
import cascading.lingual.platform.PlatformBroker;

import static java.util.Arrays.asList;

/**
 *
 */
public class FormatTarget extends CRUDTarget
  {
  public FormatTarget( Printer printer, CatalogOptions options )
    {
    super( printer, options );
    }

  @Override
  protected boolean performRename( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    String schemaName = getOptions().getSchemaName();
    Format oldFormat = Format.getFormat( getOptions().getFormatName() );
    Format newFormat = Format.getFormat( getOptions().getRenameName() );
    return catalog.renameFormat( schemaName, oldFormat, newFormat );
    }

  @Override
  protected boolean performRemove( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    String schemaName = getOptions().getSchemaName();
    Format format = Format.getFormat( getOptions().getFormatName() );
    return catalog.removeFormat( schemaName, format );
    }

  @Override
  protected String getSource( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    String schemaName = getOptions().getSchemaName();
    String formatName = getOptions().getFormatName();
    Collection<Format> formats = catalog.getSchemaDef( schemaName ).getAllFormats();
    for( Format format : formats )
      if( format.getName().equals( formatName ) )
        return formatName;

    return null;
    }

  @Override
  protected List<String> performAdd( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    Format format = Format.getFormat( getOptions().getFormatName() );

    if( format == null )
      throw new IllegalArgumentException( "add action must have a format name value" );

    String schemaName = getOptions().getSchemaName();
    Map<String, String> properties = getOptions().getProperties();
    List<String> extensions = getOptions().getExtensions();

    String providerName = getOptions().getProviderName();

    if( providerName != null )
      {
      ProviderDef providerDef = catalog.findProviderFor( schemaName, providerName );

      if( providerDef == null )
        throw new IllegalArgumentException( "provider " + providerName + " not registered to schema: " + schemaName );
      }

    catalog.addFormat( schemaName, format, extensions, properties, providerName );

    return asList( format.getName() );
    }

  @Override
  protected Collection<String> performGetNames( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    String schemaName = getOptions().getSchemaName();

    if( schemaName != null && !schemaName.isEmpty() )
      return catalog.getFormatNames( getOptions().getSchemaName() );
    else
      return catalog.getFormatNames();
    }

  @Override
  protected Map performShow( PlatformBroker platformBroker )
    {
    Format format = Format.getFormat( getOptions().getFormatName() );
    SchemaCatalog catalog = platformBroker.getCatalog();
    String schemaName = getOptions().getSchemaName();

    SchemaDef schemaDef = catalog.getSchemaDef( schemaName );

    return new FormatBuilder( schemaDef ).format( format );
    }
  }
