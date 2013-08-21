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

package cascading.lingual.catalog;

import java.util.Properties;

import cascading.lingual.LingualPlatformTestCase;
import cascading.lingual.jdbc.Driver;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.platform.PlatformBrokerFactory;
import cascading.tuple.Fields;
import org.junit.Test;

/**
 *
 */
public class CatalogPlatformTest extends LingualPlatformTestCase
  {
  @Test
  public void testPlatformBroker()
    {
    Properties properties = new Properties();

    String brokerDataPath = getOutputPath( "broker" );

    properties.setProperty( Driver.CATALOG_PROP, brokerDataPath );
    properties.setProperty( PlatformBroker.META_DATA_DIR_NAME_PROP, "_lingual" );
    properties.setProperty( PlatformBroker.CATALOG_FILE_NAME_PROP, "catalog.json" );

    PlatformBrokerFactory.instance().reloadBrokers();

    PlatformBroker broker = PlatformBrokerFactory.createPlatformBroker( getPlatformName(), properties );

    String catalogFilePath = PlatformBroker.buildPath( "/", brokerDataPath, "_lingual", "catalog.json" );

    if( broker.pathExists( catalogFilePath ) )
      broker.deletePath( catalogFilePath );

    assertFalse( "catalog loaded", broker.catalogLoaded() );

    SchemaCatalog catalog = broker.getCatalog();

    catalog.addSchemaDef( "TEST", null, null );

    catalog.createTableDefFor( "TEST", null, SALES_DEPTS_TABLE, (Fields) null, null, null );

    catalog.createStereotype( "TEST", "testStereoType", new Fields( "a", "b", "c" ) );

    assertNotNull( catalog.getSchemaDef( "TEST" ).getStereotype( "testStereoType" ) );
    assertNotNull( catalog.getSchemaDef( "TEST" ).getStereotype( "TESTSTEREOTYPE" ) );

    assertEquals( "SALES", catalog.createSchemaDefAndTableDefsFor( SALES_SCHEMA ) );

    broker.writeCatalog();

    PlatformBrokerFactory.instance().reloadBrokers();

    broker = PlatformBrokerFactory.createPlatformBroker( getPlatformName(), properties );

    catalog = broker.getCatalog();

    assertTrue( catalog.getSchemaNames().contains( "SALES" ) );
    assertTrue( catalog.getSchemaDef( "SALES" ).getChildTableNames().contains( "EMPS" ) );
    assertTrue( catalog.getSchemaDef( "SALES" ).getChildTableNames().contains( "DEPTS" ) );

    assertTrue( catalog.getSchemaNames().contains( "TEST" ) );
    assertTrue( catalog.getSchemaDef( "TEST" ).getChildTableNames().contains( "DEPTS" ) );

    assertNotNull( catalog.getSchemaDef( "TEST" ).getStereotype( "testStereoType" ) );
    assertNotNull( catalog.getSchemaDef( "TEST" ).getStereotype( "TESTSTEREOTYPE" ) );

    catalog.renameSchemaDef( "TEST", "NEWTEST" );
    assertFalse( catalog.getSchemaNames().contains( "TEST" ) );
    assertTrue( catalog.getSchemaNames().contains( "NEWTEST" ) );

    catalog.removeSchemaDef( "NEWTEST" );
    assertFalse( catalog.getSchemaNames().contains( "NEWTEST" ) );
    }
  }
