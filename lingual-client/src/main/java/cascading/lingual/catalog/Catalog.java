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

import java.io.IOException;
import java.util.Collection;

import cascading.lingual.common.Main;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.platform.PlatformBrokerFactory;

/**
 *
 */
public class Catalog extends Main<CatalogOptions>
  {
  public static void main( String[] args ) throws IOException
    {
    Catalog catalog = new Catalog();

    if( !catalog.parse( args ) )
      return;

    if( catalog.printUsage() )
      return;

    if( catalog.printVersion() )
      return;

    catalog.handle();
    }

  protected CatalogOptions createOptions()
    {
    return new CatalogOptions();
    }

  @Override
  protected boolean handle() throws IOException
    {
    PlatformBroker platformBroker = PlatformBrokerFactory.createPlatformBroker( getOptions().getPlatform() );

    if( getOptions().isInit() )
      return initializeCatalog( platformBroker );

    if( getOptions().isListSchemas() )
      return printSchemaNames( platformBroker );
    else if( getOptions().isListTables() )
      return printTableNames( platformBroker );
    else if( getOptions().isListFormats() )
      return printFormatNames( platformBroker );
    else if( getOptions().isListProtocols() )
      return printProtocolNames( platformBroker );


    if( getOptions().isSchemaActions() )
      return handleSchemaCRUD( platformBroker );
    else if( getOptions().isTableActions() )
      return handleTableCRUD( platformBroker );

    return false;
    }

  private boolean handleSchemaCRUD( PlatformBroker platformBroker )
    {
    if( getOptions().getAddURI() != null )
      return handleSchemaAdd( platformBroker );
    else if( getOptions().isRemove() )
      return handleSchemaRemove( platformBroker );
    else if( getOptions().getRenameName() != null )
      return handleSchemaRename( platformBroker );

    return false;
    }

  private boolean handleTableCRUD( PlatformBroker platformBroker )
    {
    if( getOptions().getAddURI() != null )
      return handleTableAdd( platformBroker );
    else if( getOptions().isRemove() )
      return handleTableRemove( platformBroker );
    else if( getOptions().getRenameName() != null )
      return handleTableRename( platformBroker );

    return false;
    }

  private boolean handleSchemaRename( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    boolean result = catalog.renameSchemaDef( getOptions().getSchemaName(), getOptions().getRenameName() );

    if( result )
      print( "successfully renamed schema to: %s", getOptions().getRenameName() );
    else
      print( "failed to rename schema to: %s", getOptions().getRenameName() );

    return result;
    }

  private boolean handleSchemaRemove( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    boolean result = catalog.removeSchemaDef( getOptions().getSchemaName() );

    if( result )
      print( "successfully removed schema: %s", getOptions().getSchemaName() );
    else
      print( "failed to remove schema: %s", getOptions().getSchemaName() );

    return result;
    }

  private boolean handleSchemaAdd( PlatformBroker platformBroker )
    {
    String schemaName = platformBroker.getCatalog().createSchemaFor( getOptions().getAddURI() );

    print( "added schema: %s", schemaName );

    return true;
    }

  private boolean printSchemaNames( PlatformBroker platformBroker )
    {
    print( "schema", platformBroker.getCatalog().getSchemaNames() );

    return true;
    }

  private boolean handleTableRename( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    boolean result = catalog.renameTableDef( getOptions().getSchemaName(), getOptions().getTableName(), getOptions().getRenameName() );
    if( result )
      print( "successfully renamed table to: %s", getOptions().getRenameName() );
    else
      print( "failed to rename table to: %s", getOptions().getRenameName() );

    return result;
    }

  private boolean handleTableRemove( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    boolean result = catalog.removeTableDef( getOptions().getSchemaName(), getOptions().getTableName() );

    if( result )
      print( "successfully removed table: %s", getOptions().getTableName() );
    else
      print( "failed to remove table: %s", getOptions().getTableName() );

    return result;
    }

  private boolean handleTableAdd( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    String tableName = catalog.createTableFor( getOptions().getSchemaName(), getOptions().getAddURI() );

    print( "added table: %s", tableName );

    return true;
    }

  private boolean printTableNames( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    print( "table", catalog.getTableNames( getOptions().getSchemaName() ) );

    return true;
    }

  private boolean printFormatNames( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    print( "format", catalog.getFormatNames( getOptions().getSchemaName() ) );

    return true;
    }

  private boolean printProtocolNames( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    print( "protocol", catalog.getProtocolNames( getOptions().getSchemaName() ) );

    return true;
    }

  private boolean initializeCatalog( PlatformBroker platformBroker )
    {
    boolean result = platformBroker.hasBeenInitialized();

    if( result )
      {
      print( "path: %s has already been initialized", platformBroker.getFullCatalogPath() );
      }
    else
      {
      platformBroker.writeCatalog();
      print( "path: %s has been initialized", platformBroker.getFullCatalogPath() );
      }

    return !result;
    }

  private void print( String string, String... args )
    {
    getOutPrintStream().println( String.format( string, (Object[]) args ) );
    }

  private void print( String header, Collection<String> values )
    {
    getOutPrintStream().println( header );

    getOutPrintStream().println( "-----" );

    for( String value : values )
      getOutPrintStream().println( value );
    }
  }
