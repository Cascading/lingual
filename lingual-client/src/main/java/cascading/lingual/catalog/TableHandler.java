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

import cascading.lingual.common.Printer;
import cascading.lingual.platform.PlatformBroker;

/**
 *
 */
public class TableHandler extends Handler
  {
  public TableHandler( Printer printer, CatalogOptions options )
    {
    super( printer, options );
    }

  @Override
  public boolean handle( PlatformBroker platformBroker )
    {
    if( getOptions().isListTables() )
      return handlePrint( platformBroker );

    if( getOptions().getAddURI() != null )
      return handleAdd( platformBroker );

    if( getOptions().isRemove() )
      return handleRemove( platformBroker );

    if( getOptions().getRenameName() != null )
      return handleRename( platformBroker );

    return false;
    }

  @Override
  protected boolean handleRemove( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    boolean result = catalog.removeTableDef( getOptions().getSchemaName(), getOptions().getTableName() );

    if( result )
      getPrinter().print( "successfully removed table: %s", getOptions().getTableName() );
    else
      getPrinter().print( "failed to remove table: %s", getOptions().getTableName() );

    return result;
    }

  @Override
  protected boolean handleAdd( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    String tableName = catalog.createTableFor( getOptions().getSchemaName(), getOptions().getAddURI() );

    getPrinter().print( "added table: %s", tableName );

    return true;
    }

  @Override
  protected boolean handleRename( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    boolean result = catalog.renameTableDef( getOptions().getSchemaName(), getOptions().getTableName(), getOptions().getRenameName() );
    if( result )
      getPrinter().print( "successfully renamed table to: %s", getOptions().getRenameName() );
    else
      getPrinter().print( "failed to rename table to: %s", getOptions().getRenameName() );

    return result;
    }

  @Override
  protected boolean handlePrint( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    getPrinter().print( "table", catalog.getTableNames( getOptions().getSchemaName() ) );

    return true;
    }
  }
