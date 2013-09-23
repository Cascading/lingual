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
import cascading.lingual.common.Printer;
import cascading.lingual.common.Target;
import cascading.lingual.platform.PlatformBroker;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyList;

/**
 *
 */
public abstract class CRUDTarget extends Target
  {
  private static final Logger LOG = LoggerFactory.getLogger( CRUDTarget.class );

  public CRUDTarget( Printer printer, CatalogOptions options )
    {
    super( printer, options );
    }

  public boolean updateIsNoop()
    {
    return false;
    }

  @Override
  public boolean handle( PlatformBroker platformBroker )
    {
    if( getOptions().isShow() )
      return handleShow( platformBroker );

    if( getOptions().isList() )
      return handlePrint( platformBroker );

    if( getOptions().isValidate() )
      return handleValidateDependencies( platformBroker );

    if( getOptions().isAdd() )
      return handleAdd( platformBroker );

    if( getOptions().isRemove() )
      return handleRemove( platformBroker );

    if( getOptions().isUpdate() )
      return handleUpdate( platformBroker );

    if( getOptions().isRename() )
      return handleRename( platformBroker );

    return false;
    }

  protected boolean handleAdd( PlatformBroker platformBroker )
    {
    LOG.debug( "{}: add", name );

    validateAdd( platformBroker );

    List<String> names = performAdd( platformBroker );

    for( String name : names )
      getPrinter().printFormatted( "added %s: %s", getTargetType(), name );

    return true;
    }

  protected abstract void validateAdd( PlatformBroker platformBroker );

  protected abstract List<String> performAdd( PlatformBroker platformBroker );

  protected boolean handleUpdate( PlatformBroker platformBroker )
    {
    LOG.debug( "{}: update", name );

    if( updateIsNoop() )
      return true;

    if( getSource( platformBroker ) == null )
      {
      getPrinter().printFormatted( "%s: %s does not exist or is not owned by specified schema", getTargetType(), getRequestedSourceName() );
      return false;
      }

    List<String> names = performUpdate( platformBroker );

    for( String name : names )
      getPrinter().printFormatted( "updated %s: %s", getTargetType(), name );

    return true;
    }

  protected List<String> performUpdate( PlatformBroker platformBroker )
    {
    if( performRemove( platformBroker ) )
      return performAdd( platformBroker );

    return emptyList();
    }

  protected boolean handleRename( PlatformBroker platformBroker )
    {
    LOG.debug( "{}: rename", name );

    String renameName = getOptions().getRenameName();

    if( renameName == null )
      throw new IllegalArgumentException( "rename action must have a rename target" );

    String schemaNameMsg = getOptions().getSchemaName() == null ? "" : "in schema: " + getOptions().getSchemaName() + "";

    if( getSource( platformBroker ) == null )
      {
      getPrinter().printFormatted( "%s: %s does not exist %s", getTargetType(), getRequestedSourceName(), schemaNameMsg );
      return false;
      }

    boolean result = performRename( platformBroker );

    if( result )
      getPrinter().printFormatted( "successfully renamed %s to: %s", getTargetType(), renameName );
    else
      getPrinter().printFormatted( "failed to rename %s to: %s", getTargetType(), renameName );

    return result;
    }

  protected abstract boolean performRename( PlatformBroker platformBroker );

  protected boolean handleRemove( PlatformBroker platformBroker )
    {
    LOG.debug( "{}: remove", getTargetType() );

    if( getTargetType() == null )
      throw new IllegalArgumentException( "remove action must have a remove target" );

    String schemaNameMsg = getOptions().getSchemaName() == null ? "" : "in schema: " + getOptions().getSchemaName() + "";

    if( getSource( platformBroker ) == null )
      {
      getPrinter().printFormatted( "%s: %s does not exist %s", getTargetType(), getRequestedSourceName(), schemaNameMsg );
      return false;
      }

    boolean result = performRemove( platformBroker );

    if( result )
      getPrinter().printFormatted( "successfully removed %s: %s", getTargetType(), getRequestedSourceName() );
    else
      getPrinter().printFormatted( "failed to remove %s: %s", getTargetType(), getRequestedSourceName() );

    return result;
    }

  protected abstract boolean performRemove( PlatformBroker platformBroker );

  protected abstract Object getSource( PlatformBroker platformBroker );

  protected abstract String getRequestedSourceName();

  protected boolean handlePrint( PlatformBroker platformBroker )
    {
    LOG.debug( "{}: print", getTargetType() );

    getPrinter().printLines( getTargetType(), '-', performGetNames( platformBroker ) );

    return true;
    }

  protected abstract Collection<String> performGetNames( PlatformBroker platformBroker );

  protected boolean handleValidateDependencies( PlatformBroker platformBroker )
    {
    LOG.debug( "{}: validate", getTargetType() );

    boolean result = performValidateDependencies( platformBroker );

    getPrinter().printFormatted( "%s validation returned: %b", getTargetType(), result );

    return result;
    }

  protected boolean handleShow( PlatformBroker platformBroker )
    {
    LOG.debug( "{}: show", getTargetType() );

    if( getTargetType() == null )
      throw new IllegalArgumentException( "show action must have a name" );

    Map output = performShow( platformBroker );

    if( output == null )
      {
      getPrinter().printFormatted( "%s: %s not found", getTargetType(), getRequestedSourceName() );
      return false;
      }

    getPrinter().printMap( getTargetType(), output );

    return true;
    }

  protected abstract Map performShow( PlatformBroker platformBroker );

  protected boolean performValidateDependencies( PlatformBroker platformBroker )
    {
    // only CRUD operations with external dependencies (ex. maven repo registration need to validate
    // the external dependencies are sane.
    // all other CRUD operations can only have syntax errors in the CLI which should be managed in Catalog
    // and CatalogOptions.
    return true;
    }

  protected void validateProviderName( SchemaCatalog catalog, String schemaName, String providerName )
    {
    if( catalog.findProviderFor( schemaName, providerName ) == null )
      throw new IllegalArgumentException( "provider " + providerName + " not available to schema: " + schemaName );
    }

  protected String joinOrNull( List<String> list )
    {
    if( list.isEmpty() )
      return null;

    return Joiner.on( ',' ).join( list );
    }

  protected void notGiven( Object argument, String message )
    {
    if( argument == null )
      throw new IllegalArgumentException( message );
    }
  }
