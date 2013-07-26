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
import cascading.lingual.common.Printer;
import cascading.lingual.common.Target;
import cascading.lingual.platform.PlatformBroker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    List<String> names = performAdd( platformBroker );

    for( String name : names )
      getPrinter().printFormatted( "added %s: %s", getName(), name );

    return true;
    }

  protected abstract List<String> performAdd( PlatformBroker platformBroker );

  protected boolean handleUpdate( PlatformBroker platformBroker )
    {
    LOG.debug( "{}: update", name );

    if( updateIsNoop() )
      return true;

    List<String> names = performUpdate( platformBroker );

    for( String name : names )
      getPrinter().printFormatted( "updated %s: %s", getName(), name );

    return true;
    }

  protected List<String> performUpdate( PlatformBroker platformBroker )
    {
    performRemove( platformBroker );

    return performAdd( platformBroker );
    }

  protected boolean handleRename( PlatformBroker platformBroker )
    {
    LOG.debug( "{}: rename", name );

    String renameName = getOptions().getRenameName();
    if( renameName == null )
      throw new IllegalArgumentException( "rename action must have a rename target" );

    if( getSource( platformBroker ) == null )
      throw new IllegalArgumentException( "original " + getName() + " not found for renaming" );

    boolean result = performRename( platformBroker );

    if( result )
      getPrinter().printFormatted( "successfully renamed %s to: %s", getName(), renameName );
    else
      getPrinter().printFormatted( "failed to rename %s to: %s", getName(), renameName );

    return result;
    }

  protected abstract boolean performRename( PlatformBroker platformBroker );


  protected boolean handleRemove( PlatformBroker platformBroker )
    {
    LOG.debug( "{}: remove", name );

    if( getName() == null )
      throw new IllegalArgumentException( "remove action must have a remove target" );

    if( getSource( platformBroker ) == null )
      throw new IllegalArgumentException( "original " + getName() + " not found for removal" );

    boolean result = performRemove( platformBroker );

    if( result )
      getPrinter().printFormatted( "successfully removed %s", getName() );
    else
      getPrinter().printFormatted( "failed to remove %s", getName() );

    return result;
    }

  protected abstract boolean performRemove( PlatformBroker platformBroker );

  protected abstract Object getSource( PlatformBroker platformBroker );

  protected boolean handlePrint( PlatformBroker platformBroker )
    {
    LOG.debug( "{}: print", name );

    getPrinter().printLines( getName(), '-', performGetNames( platformBroker ) );

    return true;
    }

  protected abstract Collection<String> performGetNames( PlatformBroker platformBroker );

  protected boolean handleValidateDependencies( PlatformBroker platformBroker )
    {
    LOG.debug( "{}: validate", name );

    boolean result = performValidateDependencies( platformBroker );

    getPrinter().printFormatted( "%s validation returned: %b", getName(), result );

    return result;
    }

  protected boolean handleShow( PlatformBroker platformBroker )
    {
    LOG.debug( "{}: show", name );

    Map output = performShow( platformBroker );

    getPrinter().printMap( getName(), output );

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

  }
