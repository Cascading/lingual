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

package cascading.lingual.catalog.ddl;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import cascading.lingual.catalog.CLIPlatformTestCase;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.SchemaCatalogManager;
import cascading.lingual.catalog.json.JSONFactory;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.platform.PlatformBrokerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import org.junit.Test;

/**
 *
 */
public class DDLParserPlatformTest extends CLIPlatformTestCase
  {
  static String DDL_FILE = "../lingual-platform/src/test/resources/ddl/foodmart-ddl.sql";

  static Set<String> expectedTables = new HashSet<String>();

  public static final String DDL_TEST_SCHEMA = "ddlschema";

  {
  String[] tables = {"sales_fact_1997",
                     "sales_fact_1998",
                     "sales_fact_dec_1998",
                     "inventory_fact_1997",
                     "inventory_fact_1998",
                     "agg_pl_01_sales_fact_1997",
                     "agg_ll_01_sales_fact_1997",
                     "agg_l_03_sales_fact_1997",
                     "agg_l_04_sales_fact_1997",
                     "agg_l_05_sales_fact_1997",
                     "agg_c_10_sales_fact_1997",
                     "agg_c_14_sales_fact_1997",
                     "agg_lc_100_sales_fact_1997",
                     "agg_c_special_sales_fact_1997",
                     "agg_g_ms_pcat_sales_fact_1997",
                     "agg_lc_06_sales_fact_1997",
                     "currency",
                     "account",
                     "category",
                     "customer",
                     "days",
                     "department",
                     "employee",
                     "employee_closure",
                     "expense_fact",
                     "position",
                     "product",
                     "product_class",
                     "promotion",
                     "region",
                     "reserve_employee",
                     "salary",
                     "store",
                     "store_ragged",
                     "time_by_day",
                     "warehouse",
                     "warehouse_class"};

  Collections.addAll( expectedTables, tables );
  }

  ObjectMapper mapper = JSONFactory.getObjectMapper();

  public DDLParserPlatformTest()
    {
    }

  @Test
  public void testParseAndPersistDDL() throws IOException
    {
    Properties platformProperties = getPlatformProperties();
    PlatformBroker platformBroker = PlatformBrokerFactory.createPlatformBroker( getPlatformName(), platformProperties );

    copyFromLocal( DDL_FILE );

    initCatalog();

    catalog( "--schema", DDL_TEST_SCHEMA, "--add", getSchemaPath( DDL_TEST_SCHEMA ) );

    SchemaCatalog catalog = getSchemaCatalog();
    SchemaCatalogManager schemaCatalogManager = platformBroker.getCatalogManager();

    Protocol defaultProtocol = catalog.getSchemaDef( DDL_TEST_SCHEMA ).findDefaultProtocol();

    DDLParser parser = new DDLParser( schemaCatalogManager, DDL_TEST_SCHEMA, defaultProtocol.toString() , "csv" );

    File testFile = new File( DDL_FILE );

    assertTrue( "test file " + testFile + "not found in " + new File( "" ).getAbsolutePath() , testFile.exists() );

    parser.apply( new File( DDL_FILE ) );

    Set<String> tables = new HashSet<String>( catalog.getTableNames( DDL_TEST_SCHEMA ) );

    assertTrue( Sets.difference( tables, expectedTables ).size() == 0 );

    String jsonFirst = writeObject( catalog );

    TestSchemaCatalog firstRead = readCatalog( jsonFirst );

    assertEquals( catalog, firstRead );

    String jsonSecond = writeObject( firstRead );

    TestSchemaCatalog secondRead = readCatalog( jsonSecond );

    assertEquals( firstRead, secondRead );
    }

  private TestSchemaCatalog readCatalog( String json ) throws IOException
    {
    StringReader reader = new StringReader( json );
    return mapper.readValue( reader, TestSchemaCatalog.class );
    }

  private String writeObject( SchemaCatalog wroteCatalog ) throws IOException
    {
    StringWriter writer = new StringWriter();
    mapper.writer().withDefaultPrettyPrinter().writeValue( writer, wroteCatalog );

    return writer.toString();
    }
  }
