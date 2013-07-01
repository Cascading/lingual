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
import java.util.Set;

import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.json.JSONFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import junit.framework.TestCase;

/**
 *
 */
public class DDLParserTest extends TestCase
  {
  static String file = "src/test/resources/ddl/foodmart-ddl.sql";

  static Set<String> expectedTables = new HashSet<String>();

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

  public DDLParserTest()
    {
    }

  public void testParseAndPersistDDL() throws IOException
    {
    TestSchemaCatalog catalog = new TestSchemaCatalog( Protocol.getProtocol( "file" ), Format.getFormat( "csv" ) );

    catalog.createSchemaDef( "test", null, null, null );
    catalog.initializeNew();

    DDLParser parser = new DDLParser( catalog, "test", "test", "file", "csv" );

    parser.apply( new File( file ) );

    Set<String> tables = new HashSet<String>( catalog.getTableNames( "test" ) );

    assertTrue( Sets.difference( tables, expectedTables ).size() == 0 );

    String jsonFirst = writeObject( catalog );

//    System.out.println( jsonFirst );

    TestSchemaCatalog firstRead = readCatalog( jsonFirst );

    assertEquals( catalog, firstRead );

    String jsonSecond = writeObject( firstRead );

//    System.out.println( jsonSecond );

    TestSchemaCatalog secondRead = readCatalog( jsonSecond );

    assertEquals( firstRead, secondRead );
    }

  private TestSchemaCatalog readCatalog( String json ) throws IOException
    {
    StringReader reader = new StringReader( json );
    return mapper.readValue( reader, TestSchemaCatalog.class );
    }

  private String writeObject( TestSchemaCatalog wroteCatalog ) throws IOException
    {
    StringWriter writer = new StringWriter();
    mapper.writer().withDefaultPrettyPrinter().writeValue( writer, wroteCatalog );

    return writer.toString();
    }
  }
