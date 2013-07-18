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

import java.io.IOException;
import java.util.Collection;

import com.google.common.base.Joiner;
import org.junit.Test;

/**
 *
 */
public class CatalogCLIPlatformTest extends CLIPlatformTestCase
  {
  private static final String AD_HOC_SCHEMA = "adhoc";
  private static final String AD_HOC_SCHEMA_LC = AD_HOC_SCHEMA.toLowerCase();
  private static final String AD_HOC_SCHEMA_UC = AD_HOC_SCHEMA.toUpperCase();
  private static final String AD_HOC_SCHEMA_MC = "aDHOc";
  private static final String TEST_TABLE_NAME = "local";
  private static final String TEST_TABLE_NAME_LC = TEST_TABLE_NAME.toLowerCase();
  private static final String TEST_TABLE_NAME_UC = TEST_TABLE_NAME.toUpperCase();
  private static final String TEST_TABLE_NAME_MC = "lOcAl";

  @Test
  public void testCLI() throws IOException
    {
    initCatalog();

    SchemaCatalog schemaCatalog = getSchemaCatalog();

    catalog( "--schema", "sales", "--add", SALES_SCHEMA );

    Collection<String> schemaNames = schemaCatalog.getSchemaNames();
    assertTrue( "sales schema not found in " + schemaNames.toString(), schemaNames.contains( "sales" ) );

    catalog(
      "--stereotype", "emps", "--add",
      "--columns", Joiner.on( "," ).join( EMPS_COLUMNS ),
      "--types", Joiner.on( "," ).join( EMPS_COLUMN_TYPES )
    );

    Collection<String> stereotypeNames = schemaCatalog.getStereotypeNames();
    assertTrue( "stereotype not added to catalog in " + stereotypeNames.toString(), stereotypeNames.contains( "emps" ) );
    assertFalse( "bogus stereotype found in catalog of " + stereotypeNames.toString(), stereotypeNames.contains( "xx_emps" ) );

    catalog( "--stereotype" );

    catalog( "--schema", AD_HOC_SCHEMA, "--add" );
    catalog( "--schema", AD_HOC_SCHEMA, "--table", TEST_TABLE_NAME, "--add", SALES_EMPS_TABLE, "--stereotype", "emps" );
    catalog( "--schema", AD_HOC_SCHEMA, "--table" );

    Collection<String> tableNames = schemaCatalog.getTableNames( AD_HOC_SCHEMA );
    assertTrue( AD_HOC_SCHEMA + " does not contain table " + TEST_TABLE_NAME + " in " + tableNames.toString(), tableNames.contains( TEST_TABLE_NAME ) );

    catalog( "--schema", AD_HOC_SCHEMA, "--format", "table", "--add", "--extensions", ".jdbc,.jdbc.lzo" );

    catalog( "--schema", AD_HOC_SCHEMA, "--protocol", "jdbc", "--add", "--uris", "jdbc:,jdbcs:" );

    catalog( "--schema", AD_HOC_SCHEMA,
      "--table", "remote", "--add", SALES_EMPS_TABLE,
      "--stereotype", "emps",
      "--format", "table", "--protocol", "jdbc"
    );
    catalog( "--schema", AD_HOC_SCHEMA, "--table" );

    catalog( "--schema" );
    }

  @Test
  public void testCLICaseSensitivity() throws IOException
    {
    initCatalog();
    // general process for test: create it with mixed-case name, search for it with different mixed-case name
    // to see if that works but confirm that the getDefNames() preserves case.

    SchemaCatalog schemaCatalog = getSchemaCatalog();
    Collection<String> schemaNames = schemaCatalog.getSchemaNames();
    assertEquals( "Catalog contains schema at startup " + schemaNames.toString(), 0, schemaNames.size() );

    // add the schema twice under different cases should get an error and produce only one with original case
    catalog( "--schema", AD_HOC_SCHEMA_MC, "--add" );
    catalogWithOptionalTest( false, "--schema", AD_HOC_SCHEMA_UC, "--add" );
    schemaNames = schemaCatalog.getSchemaNames();
    assertEquals( "Case change should still have one schemas: " + schemaNames.toString(), 1, schemaNames.size() );
    assertEquals( "Case was not preserved for schema", AD_HOC_SCHEMA_MC, schemaNames.iterator().next() );

    // should be able to use a mixed case-name for work against a schema and adding table under multiple cases
    // should produce only one table.
    Collection<String> tableNames = schemaCatalog.getTableNames( AD_HOC_SCHEMA_LC );
    assertEquals( "Schema had tables at startup: " + tableNames.toString(), 0, tableNames.size() );
    catalog( "--schema", AD_HOC_SCHEMA_MC, "--table", TEST_TABLE_NAME_MC, "--add", SALES_EMPS_TABLE, "--stereotype", "emps" );
    catalogWithOptionalTest( false, "--schema", AD_HOC_SCHEMA_UC, "--table", TEST_TABLE_NAME_LC, "--add", SALES_EMPS_TABLE, "--stereotype", "emps" );
    tableNames = schemaCatalog.getTableNames( AD_HOC_SCHEMA_LC );
    assertEquals( "Case change should still have one table: " + tableNames.toString(), 1, tableNames.size() );
    assertEquals( "Case was not preserved for table", TEST_TABLE_NAME_MC, tableNames.iterator().next() );

    // should be able to find the correct table with assorted case searches by name
    SchemaDef schemaDef = schemaCatalog.getSchemaDef( AD_HOC_SCHEMA_LC );
    assertEquals( "Wrong schema found in full search", AD_HOC_SCHEMA_MC, schemaDef.getName() );
    TableDef tableDef = schemaDef.getTable( TEST_TABLE_NAME_UC );
    assertNotNull( "No table found for mixed case search in: " + schemaDef.getChildTableNames().toString(), tableDef );
    assertEquals( "Wrong table found in full search", TEST_TABLE_NAME_MC, tableDef.getName() );
    }
  }
