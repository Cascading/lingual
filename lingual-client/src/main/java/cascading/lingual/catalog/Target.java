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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class Target
  {
  private static final Logger LOG = LoggerFactory.getLogger( Target.class );

  private String name = getClass().getSimpleName().replaceAll( "Target$", "" ).toLowerCase();
  private final Printer printer;
  private final CatalogOptions options;

  public Target( Printer printer, CatalogOptions options )
    {
    this.printer = printer;
    this.options = options;
    }

  public String getName()
    {
    return name;
    }

  public Printer getPrinter()
    {
    return printer;
    }

  public CatalogOptions getOptions()
    {
    return options;
    }

  public boolean handle( PlatformBroker platformBroker )
    {
    if( getOptions().isList() )
      return handlePrint( platformBroker );

    if( getOptions().isAdd() )
      return handleAdd( platformBroker );

    if( getOptions().isRemove() )
      return handleRemove( platformBroker );

    if( getOptions().getRenameName() != null )
      return handleRename( platformBroker );

    return false;
    }

  protected boolean handleRename( PlatformBroker platformBroker )
    {
    LOG.debug( "{}: rename", name );

    boolean result = performRename( platformBroker );

    if( result )
      getPrinter().print( "successfully renamed %s to: %s", getName(), getOptions().getRenameName() );
    else
      getPrinter().print( "failed to rename %s to: %s", getName(), getOptions().getRenameName() );

    return result;
    }

  protected abstract boolean performRename( PlatformBroker platformBroker );

  protected boolean handleRemove( PlatformBroker platformBroker )
    {
    LOG.debug( "{}: remove", name );

    boolean result = performRemove( platformBroker );

    if( result )
      getPrinter().print( "successfully removed %s: %s", getName(), getOptions().getSchemaName() );
    else
      getPrinter().print( "failed to remove %s: %s", getName(), getOptions().getSchemaName() );

    return result;
    }

  protected abstract boolean performRemove( PlatformBroker platformBroker );

  protected boolean handleAdd( PlatformBroker platformBroker )
    {
    LOG.debug( "{}: add", name );

    String schemaName = performAdd( platformBroker );

    getPrinter().print( "added %s: %s", getName(), schemaName );

    return true;
    }

  protected abstract String performAdd( PlatformBroker platformBroker );

  protected boolean handlePrint( PlatformBroker platformBroker )
    {
    LOG.debug( "{}: print", name );

    getPrinter().print( getName(), performGetNames( platformBroker ) );

    return true;
    }

  protected abstract Collection<String> performGetNames( PlatformBroker platformBroker );
  }
