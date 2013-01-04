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

import java.util.Properties;

import cascading.lingual.LingualPlatformTestCase;
import cascading.lingual.jdbc.Driver;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.platform.PlatformBrokerFactory;
import org.junit.Test;

/**
 *
 */
public class CatalogPlatformTestCase extends LingualPlatformTestCase
  {
  @Test
  public void testPlatformBroker()
    {
    Properties properties = new Properties();

    String brokerDataPath = getOutputPath( "broker" );

    properties.setProperty( Driver.CATALOG_PROP, brokerDataPath );
    properties.setProperty( PlatformBroker.META_DATA_PATH_PROP, "_lingual" );
    properties.setProperty( PlatformBroker.CATALOG_FILE_PROP, "catalog.ser" );

    PlatformBroker broker = PlatformBrokerFactory.createPlatformBroker( getPlatformName(), properties );

    String catalogFilePath = PlatformBroker.makePath( "/", brokerDataPath, "_lingual", "catalog.ser" );

    if( broker.pathExists( catalogFilePath ) )
      broker.deletePath( catalogFilePath );

    SchemaCatalog catalog = broker.getCatalog();

    catalog.addSchemaDef( "test" );

    catalog.createTableDefFor( "test", null, DEPTS_TABLE, null, null, null );

    assertEquals( "SALES", catalog.createSchemaDefAndTableDefsFor( SALES_SCHEMA ) );

    broker.writeCatalog();

    PlatformBrokerFactory.instance().reloadBrokers();

    broker = PlatformBrokerFactory.createPlatformBroker( getPlatformName(), properties );

    catalog = broker.getCatalog();

    assertTrue( catalog.getSchemaNames().contains( "SALES" ) );
    assertTrue( catalog.getSchemaDef( "SALES" ).getChildTableNames().contains( "EMPS" ) );

    assertTrue( catalog.getSchemaNames().contains( "test" ) );
    assertTrue( catalog.getSchemaDef( "test" ).getChildTableNames().contains( "DEPTS" ) );

    catalog.renameSchemaDef( "test", "newtest" );
    assertFalse( catalog.getSchemaNames().contains( "test" ) );
    assertTrue( catalog.getSchemaNames().contains( "newtest" ) );

    catalog.removeSchemaDef( "newtest" );
    assertFalse( catalog.getSchemaNames().contains( "newtest" ) );
    }
  }
