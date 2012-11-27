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
public class SchemaHandler extends Handler
  {
  public SchemaHandler( Printer printer, CatalogOptions options )
    {
    super( printer, options );
    }

  @Override
  public boolean handle( PlatformBroker platformBroker )
    {
    if( getOptions().isListSchemas() )
      return handlePrint( platformBroker );

    if( getOptions().isAdd() )
      return handleAdd( platformBroker );
    else if( getOptions().isRemove() )
      return handleRemove( platformBroker );
    else if( getOptions().getRenameName() != null )
      return handleRename( platformBroker );

    return false;
    }

  @Override
  protected boolean handleRename( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    boolean result = catalog.renameSchemaDef( getOptions().getSchemaName(), getOptions().getRenameName() );

    if( result )
      getPrinter().print( "successfully renamed schema to: %s", getOptions().getRenameName() );
    else
      getPrinter().print( "failed to rename schema to: %s", getOptions().getRenameName() );

    return result;
    }

  @Override
  protected boolean handleRemove( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    boolean result = catalog.removeSchemaDef( getOptions().getSchemaName() );

    if( result )
      getPrinter().print( "successfully removed schema: %s", getOptions().getSchemaName() );
    else
      getPrinter().print( "failed to remove schema: %s", getOptions().getSchemaName() );

    return result;
    }

  @Override
  protected boolean handleAdd( PlatformBroker platformBroker )
    {
    String schemaName = platformBroker.getCatalog().createSchemaFor( getOptions().getAddURI() );

    getPrinter().print( "added schema: %s", schemaName );

    return true;
    }

  @Override
  protected boolean handlePrint( PlatformBroker platformBroker )
    {
    getPrinter().print( "schema", platformBroker.getCatalog().getSchemaNames() );

    return true;
    }
  }
