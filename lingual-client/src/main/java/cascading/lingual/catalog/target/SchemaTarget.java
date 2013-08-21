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
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.SchemaDef;
import cascading.lingual.catalog.builder.SchemaBuilder;
import cascading.lingual.common.Printer;
import cascading.lingual.platform.PlatformBroker;

import static java.util.Arrays.asList;

/**
 *
 */
public class SchemaTarget extends CRUDTarget
  {
  public SchemaTarget( Printer printer, CatalogOptions options )
    {
    super( printer, options );
    }

  @Override
  public boolean updateIsNoop()
    {
    return true;
    }

  @Override
  protected boolean performRename( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    String schemaName = getOptions().getSchemaName();
    String renameName = getOptions().getRenameName();

    return catalog.renameSchemaDef( schemaName, renameName );
    }

  @Override
  protected boolean performRemove( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    String schemaName = getOptions().getSchemaName();

    return catalog.removeSchemaDef( schemaName );
    }

  @Override
  protected Object getSource( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    return catalog.getSchemaDef( getOptions().getSchemaName() );
    }

  @Override
  protected String getRequestedSourceName()
    {
    return getOptions().getSchemaName();
    }

  @Override
  protected void validateAdd( PlatformBroker platformBroker )
    {
    }

  @Override
  protected List<String> performAdd( PlatformBroker platformBroker )
    {
    String addURI = getOptions().getAddOrUpdateURI();
    SchemaCatalog catalog = platformBroker.getCatalog();
    String schemaName = getOptions().getSchemaName();
    String protocol = getOptions().getProtocolName();
    String format = getOptions().getFormatName();

    if( addURI == null )
      {
      boolean success = catalog.addSchemaDef( schemaName, protocol, format );

      if( success )
        return asList( schemaName );

      return null;
      }

    return asList( catalog.createSchemaDefAndTableDefsFor( schemaName, protocol, format, addURI, false ) );
    }

  @Override
  protected Collection<String> performGetNames( PlatformBroker platformBroker )
    {
    return platformBroker.getCatalog().getSchemaNames();
    }

  @Override
  protected Map performShow( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    String schemaName = getOptions().getSchemaName();
    SchemaDef schemaDef = catalog.getSchemaDefChecked( schemaName );

    return new SchemaBuilder().format( schemaDef );
    }
  }
