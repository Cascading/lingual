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
  protected void handle() throws IOException
    {
    PlatformBroker platformBroker = PlatformBrokerFactory.createPlatformBroker( getOptions().getPlatform() );
    SchemaCatalog schemaCatalog = platformBroker.getCatalog();

//    if(getOptions().isListSchemas())
//      print( lingualCatalog.getSchemaFor(  )


    }
  }
