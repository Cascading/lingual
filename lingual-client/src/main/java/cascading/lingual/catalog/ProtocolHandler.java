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
public class ProtocolHandler extends Handler
  {
  public ProtocolHandler( Printer printer, CatalogOptions options )
    {
    super( printer, options );
    }

  @Override
  public boolean handle( PlatformBroker platformBroker )
    {
    return false;
    }

  @Override
  protected boolean handleRename( PlatformBroker platformBroker )
    {
    return false;
    }

  @Override
  protected boolean handleRemove( PlatformBroker platformBroker )
    {
    return false;
    }

  @Override
  protected boolean handleAdd( PlatformBroker platformBroker )
    {
    return false;
    }

  @Override
  protected boolean handlePrint( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    getPrinter().print( "protocol", catalog.getProtocolNames( getOptions().getSchemaName() ) );

    return true;
    }

  }
