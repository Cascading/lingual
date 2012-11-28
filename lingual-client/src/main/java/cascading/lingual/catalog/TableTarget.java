/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.lingual.catalog;

import java.util.Collection;

import cascading.lingual.common.Printer;
import cascading.lingual.platform.PlatformBroker;

/**
 *
 */
public class TableTarget extends Target
  {
  public TableTarget( Printer printer, CatalogOptions options )
    {
    super( printer, options );
    }

  @Override
  protected boolean performRename( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    return catalog.renameTableDef( getOptions().getSchemaName(), getOptions().getTableName(), getOptions().getRenameName() );
    }

  @Override
  protected boolean performRemove( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    return catalog.removeTableDef( getOptions().getSchemaName(), getOptions().getTableName() );
    }

  @Override
  protected String performAdd( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    String addURI = getOptions().getAddURI();

    if( addURI == null )
      throw new IllegalArgumentException( "add action must have a uri value" );

    String tableName = getOptions().getTableName();
    Protocol protocol = Protocol.getProtocol( getOptions().getProtocolName() );
    Format format = Format.getFormat( getOptions().getFormatName() );

    return catalog.createTableDefFor( catalog.getRootSchemaDef().getOrAddSchema( getOptions().getSchemaName() ), tableName, addURI, protocol, format );
    }

  @Override
  protected Collection<String> performGetNames( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    return catalog.getTableNames( getOptions().getSchemaName() );
    }
  }
