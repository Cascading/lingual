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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
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
  private static final String EMPS_STEREOTYPE_NAME = "emps";
  private static final String TABLE_FORMAT_NAME = "table";
  private static final String JDBC_PROTOCOL_NAME = "jdbc";
  private static final String RENAME_FROM_SUFFIX = "_fr";
  private static final String RENAME_TO_SUFFIX = "_to";

  @Test
  public void testCLI() throws IOException
    {
    initCatalog();

    SchemaCatalog schemaCatalog = getSchemaCatalog();

    catalog( "--schema", "sales", "--add", SALES_SCHEMA );

    Collection<String> schemaNames = schemaCatalog.getSchemaNames();
    assertTrue( "sales schema not found in " + schemaNames.toString(), schemaNames.contains( "sales" ) );

    catalog(
      "--stereotype", EMPS_STEREOTYPE_NAME, "--add",
      "--columns", Joiner.on( "," ).join( EMPS_COLUMNS ),
      "--types", Joiner.on( "," ).join( EMPS_COLUMN_TYPES )
    );

    Collection<String> stereotypeNames = schemaCatalog.getStereotypeNames();
    assertTrue( "stereotype not added to catalog in " + stereotypeNames.toString(), stereotypeNames.contains( "emps" ) );
    assertFalse( "bogus stereotype found in catalog of " + stereotypeNames.toString(), stereotypeNames.contains( "xx_emps" ) );

    catalog( "--stereotype" );

    catalog( "--schema", AD_HOC_SCHEMA, "--add", getSchemaPath( AD_HOC_SCHEMA ) );
    catalog( "--schema", AD_HOC_SCHEMA, "--table", TEST_TABLE_NAME, "--add", SALES_EMPS_TABLE, "--stereotype", EMPS_STEREOTYPE_NAME );
    catalog( "--schema", AD_HOC_SCHEMA, "--table" );

    Collection<String> tableNames = schemaCatalog.getTableNames( AD_HOC_SCHEMA );
    assertTrue( AD_HOC_SCHEMA + " does not contain table " + TEST_TABLE_NAME + " in " + tableNames.toString(), tableNames.contains( TEST_TABLE_NAME ) );

    catalog( false, "--schema", AD_HOC_SCHEMA, "--table", TEST_TABLE_NAME, "--add", SALES_EMPS_TABLE, "--stereotype", EMPS_STEREOTYPE_NAME,
      "--properties", "foo=bar" );

    catalog( "--schema", AD_HOC_SCHEMA, "--format", TABLE_FORMAT_NAME, "--add", "--extensions", ".jdbc,.jdbc.lzo", "--provider", "text", "--properties", "someProperty=someValue" );

    assertEquals( Arrays.asList( "someValue" ), schemaCatalog.getFormatProperty( AD_HOC_SCHEMA, TABLE_FORMAT_NAME, "someProperty" ) );

    catalog( "--schema", AD_HOC_SCHEMA, "--format", TABLE_FORMAT_NAME, "--update", "--properties", "someProperty2=someValue2" );

    assertEquals( Arrays.asList( "someValue" ), schemaCatalog.getFormatProperty( AD_HOC_SCHEMA, TABLE_FORMAT_NAME, "someProperty" ) );
    assertEquals( Arrays.asList( "someValue2" ), schemaCatalog.getFormatProperty( AD_HOC_SCHEMA, TABLE_FORMAT_NAME, "someProperty2" ) );

    catalog( "--schema", AD_HOC_SCHEMA, "--protocol", JDBC_PROTOCOL_NAME, "--add", "--schemes", "jdbc:,jdbcs:", "--provider", "text" );

    catalog( "--schema", AD_HOC_SCHEMA,
      "--table", "remote", "--add", SALES_EMPS_TABLE,
      "--stereotype", EMPS_STEREOTYPE_NAME,
      "--format", TABLE_FORMAT_NAME, "--protocol", JDBC_PROTOCOL_NAME
    );
    catalog( "--schema", AD_HOC_SCHEMA, "--table" );

    catalog( "--schema" );

    catalog( "--stereotype", EMPS_STEREOTYPE_NAME, "--remove" );
    stereotypeNames = schemaCatalog.getStereotypeNames();
    assertFalse( "stereotype found in catalog after removal " + stereotypeNames.toString(), stereotypeNames.contains( "emps" ) );

    catalog( "--schema", AD_HOC_SCHEMA, "--format", TABLE_FORMAT_NAME, "--remove" );

    catalog( "--schema", AD_HOC_SCHEMA, "--protocol", JDBC_PROTOCOL_NAME, "--remove" );

    catalog( "--schema", AD_HOC_SCHEMA, "--table", TEST_TABLE_NAME, "--remove" );
    tableNames = schemaCatalog.getTableNames( AD_HOC_SCHEMA );
    assertFalse( AD_HOC_SCHEMA + " still contain tables " + TEST_TABLE_NAME + " in " + tableNames.toString(), tableNames.contains( TEST_TABLE_NAME ) );

    catalog( "--schema", AD_HOC_SCHEMA, "--remove" );
    schemaNames = schemaCatalog.getSchemaNames();
    assertFalse( AD_HOC_SCHEMA + " still found in " + schemaNames.toString(), schemaNames.contains( AD_HOC_SCHEMA ) );
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
    catalog( false, "--schema", AD_HOC_SCHEMA_UC, "--add" );
    schemaNames = schemaCatalog.getSchemaNames();
    assertEquals( "Case change should still have one schemas: " + schemaNames.toString(), 1, schemaNames.size() );
    assertEquals( "Case was not preserved for schema", AD_HOC_SCHEMA_MC, schemaNames.iterator().next() );

    // should be able to use a mixed case-name for work against a schema and adding table under multiple cases
    // should produce only one table.
    Collection<String> tableNames = schemaCatalog.getTableNames( AD_HOC_SCHEMA_LC );
    assertEquals( "Schema had tables at startup: " + tableNames.toString(), 0, tableNames.size() );
    catalog( "--schema", AD_HOC_SCHEMA_MC, "--table", TEST_TABLE_NAME_MC, "--add", SALES_EMPS_TABLE, "--stereotype", EMPS_STEREOTYPE_NAME );
    catalog( false, "--schema", AD_HOC_SCHEMA_UC, "--table", TEST_TABLE_NAME_LC, "--add", SALES_EMPS_TABLE, "--stereotype", EMPS_STEREOTYPE_NAME );
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

  @Test
  public void testRenameCLI() throws IOException
    {
    initCatalog();

    SchemaCatalog schemaCatalog = getSchemaCatalog();

    // setup and populate
    catalog(
      "--stereotype", EMPS_STEREOTYPE_NAME + RENAME_FROM_SUFFIX, "--add",
      "--columns", Joiner.on( "," ).join( EMPS_COLUMNS ),
      "--types", Joiner.on( "," ).join( EMPS_COLUMN_TYPES )
    );
    catalog( "--schema", AD_HOC_SCHEMA + RENAME_FROM_SUFFIX, "--add" );

    catalog( "--schema", AD_HOC_SCHEMA + RENAME_FROM_SUFFIX, "--table", TEST_TABLE_NAME + RENAME_FROM_SUFFIX,
      "--add", SALES_EMPS_TABLE, "--stereotype", EMPS_STEREOTYPE_NAME + RENAME_FROM_SUFFIX );

    catalog( "--schema", AD_HOC_SCHEMA + RENAME_FROM_SUFFIX, "--format", TABLE_FORMAT_NAME + RENAME_FROM_SUFFIX,
      "--add", "--extensions", ".jdbc,.jdbc.lzo", "--provider", "text" );

    catalog( "--schema", AD_HOC_SCHEMA + RENAME_FROM_SUFFIX, "--protocol", JDBC_PROTOCOL_NAME + RENAME_FROM_SUFFIX,
      "--add", "--schemes", "jdbc:,jdbcs:", "--provider", "text" );

    Collection<String> schemaNames = schemaCatalog.getSchemaNames();
    assertTrue( "initial schema missing from: " + schemaNames.toString(), schemaNames.contains( "adhoc_fr" ) );
    Collection<String> tableNames = schemaCatalog.getTableNames( "adhoc_fr" );
    assertTrue( "initial table missing from: " + tableNames.toString(), tableNames.contains( "local_fr" ) );
    Collection<String> formatNames = schemaCatalog.getFormatNames( "adhoc_fr" );
    assertTrue( "initial format missing from: " + formatNames.toString(), formatNames.contains( "table_fr" ) );
    Collection<String> protocolNames = schemaCatalog.getProtocolNames( "adhoc_fr" );
    assertTrue( "initial protocol missing from: " + protocolNames.toString(), protocolNames.contains( "jdbc_fr" ) );
    Collection<String> stereotypeNames = schemaCatalog.getStereotypeNames();
    assertTrue( "initial stereotype missing from: " + stereotypeNames.toString(), stereotypeNames.contains( "emps_fr" ) );

    // renaming non-existing targets should fail
    catalog( false, "--schema", AD_HOC_SCHEMA + RENAME_FROM_SUFFIX, "--protocol", "fakeprotocol", "--rename", JDBC_PROTOCOL_NAME + RENAME_TO_SUFFIX );
    catalog( false, "--schema", AD_HOC_SCHEMA + RENAME_FROM_SUFFIX, "--format", "fakeformat", "--rename", TABLE_FORMAT_NAME + RENAME_TO_SUFFIX );
    catalog( false, "--schema", AD_HOC_SCHEMA + RENAME_FROM_SUFFIX, "--table", "faketable", "--rename", TEST_TABLE_NAME + RENAME_TO_SUFFIX );
    catalog( false, "--stereotype", "fakestereotype", "--rename", EMPS_STEREOTYPE_NAME + RENAME_TO_SUFFIX );
    catalog( false, "--schema", "fakeschema", "--rename", AD_HOC_SCHEMA + RENAME_TO_SUFFIX );

    // renaming targets in wrong schema should fail
    catalog( false, "--schema", "fakeschema", "--table", TABLE_FORMAT_NAME + RENAME_FROM_SUFFIX, "--rename", TEST_TABLE_NAME + RENAME_TO_SUFFIX );
    catalog( false, "--schema", "fakeschema", "--stereotype", EMPS_STEREOTYPE_NAME + RENAME_FROM_SUFFIX, "--rename", EMPS_STEREOTYPE_NAME + RENAME_TO_SUFFIX );
    catalog( false, "--schema", "fakeschema", "--rename", AD_HOC_SCHEMA + RENAME_TO_SUFFIX );

    // valid renames should work
    catalog( "--schema", AD_HOC_SCHEMA + RENAME_FROM_SUFFIX, "--protocol", JDBC_PROTOCOL_NAME + RENAME_FROM_SUFFIX, "--rename", JDBC_PROTOCOL_NAME + RENAME_TO_SUFFIX );
    protocolNames = getSchemaCatalog().getProtocolNames( "adhoc_fr" );
    assertTrue( "renamed protocol missing from: " + protocolNames.toString(), protocolNames.contains( "jdbc_to" ) );
    assertFalse( "original protocol found in : " + protocolNames.toString(), protocolNames.contains( JDBC_PROTOCOL_NAME + RENAME_FROM_SUFFIX ) );

    catalog( "--schema", AD_HOC_SCHEMA + RENAME_FROM_SUFFIX, "--format", TABLE_FORMAT_NAME + RENAME_FROM_SUFFIX, "--rename", TABLE_FORMAT_NAME + RENAME_TO_SUFFIX );
    formatNames = getSchemaCatalog().getFormatNames( "adhoc_fr" );
    assertTrue( "renamed format missing from: " + formatNames.toString(), formatNames.contains( "table_to" ) );
    assertFalse( "original format found in : " + formatNames.toString(), formatNames.contains( TABLE_FORMAT_NAME + RENAME_FROM_SUFFIX ) );

    catalog( "--schema", AD_HOC_SCHEMA + RENAME_FROM_SUFFIX, "--table", TEST_TABLE_NAME + RENAME_FROM_SUFFIX, "--rename", TEST_TABLE_NAME + RENAME_TO_SUFFIX );
    tableNames = getSchemaCatalog().getTableNames( "adhoc_fr" );
    assertTrue( "renamed table missing from: " + tableNames.toString(), tableNames.contains( "local_to" ) );
    assertFalse( "original table found in : " + tableNames.toString(), tableNames.contains( TEST_TABLE_NAME + RENAME_FROM_SUFFIX ) );

    catalog( "--stereotype", EMPS_STEREOTYPE_NAME + RENAME_FROM_SUFFIX, "--rename", EMPS_STEREOTYPE_NAME + RENAME_TO_SUFFIX );
    stereotypeNames = getSchemaCatalog().getStereotypeNames();
    assertTrue( "renamed stereotype missing from: " + stereotypeNames.toString(), stereotypeNames.contains( "emps_to" ) );
    assertFalse( "original stereotype found in : " + stereotypeNames.toString(), stereotypeNames.contains( EMPS_STEREOTYPE_NAME + RENAME_FROM_SUFFIX ) );

    catalog( "--schema", AD_HOC_SCHEMA + RENAME_FROM_SUFFIX, "--rename", AD_HOC_SCHEMA + RENAME_TO_SUFFIX );
    schemaNames = getSchemaCatalog().getSchemaNames();
    assertTrue( "renamed schema missing from: " + schemaNames.toString(), schemaNames.contains( "adhoc_to" ) );
    assertFalse( "original schema found in : " + schemaNames.toString(), schemaNames.contains( AD_HOC_SCHEMA + RENAME_FROM_SUFFIX ) );
    }

  @Test
  public void testRenamePropagation() throws IOException
    {
    initCatalog();

    SchemaCatalog schemaCatalog = getSchemaCatalog();

    // setup and populate
    catalog(
      "--stereotype", EMPS_STEREOTYPE_NAME + RENAME_FROM_SUFFIX, "--add",
      "--columns", Joiner.on( "," ).join( EMPS_COLUMNS ),
      "--types", Joiner.on( "," ).join( EMPS_COLUMN_TYPES )
    );
    catalog( "--schema", AD_HOC_SCHEMA + RENAME_FROM_SUFFIX, "--add" );
    catalog( "--schema", AD_HOC_SCHEMA + RENAME_FROM_SUFFIX, "--table", TEST_TABLE_NAME + RENAME_FROM_SUFFIX, "--add", SALES_EMPS_TABLE, "--stereotype", EMPS_STEREOTYPE_NAME + RENAME_FROM_SUFFIX );
    catalog( "--schema", AD_HOC_SCHEMA + RENAME_FROM_SUFFIX, "--format", TABLE_FORMAT_NAME + RENAME_FROM_SUFFIX, "--add", "--extensions", ".jdbc,.jdbc.lzo", "--provider", "text" );
    catalog( "--schema", AD_HOC_SCHEMA + RENAME_FROM_SUFFIX, "--protocol", JDBC_PROTOCOL_NAME + RENAME_FROM_SUFFIX, "--add", "--schemes", "jdbc:,jdbcs:", "--provider", "text" );
    catalog( "--schema", AD_HOC_SCHEMA + RENAME_FROM_SUFFIX,
      "--table", "remote_test", "--add", SALES_EMPS_TABLE,
      "--stereotype", EMPS_STEREOTYPE_NAME + RENAME_FROM_SUFFIX,
      "--format", TABLE_FORMAT_NAME + RENAME_FROM_SUFFIX, "--protocol", JDBC_PROTOCOL_NAME + RENAME_FROM_SUFFIX
    );

    Collection<String> schemaNames = schemaCatalog.getSchemaNames();
    assertTrue( "initial schema missing from: " + schemaNames.toString(), schemaNames.contains( "adhoc_fr" ) );
    Collection<String> tableNames = schemaCatalog.getTableNames( "adhoc_fr" );
    assertTrue( "initial table missing from: " + tableNames.toString(), tableNames.contains( "local_fr" ) );
    Collection<String> formatNames = schemaCatalog.getFormatNames( "adhoc_fr" );
    assertTrue( "initial format missing from: " + formatNames.toString(), formatNames.contains( "table_fr" ) );
    Collection<String> protocolNames = schemaCatalog.getProtocolNames( "adhoc_fr" );
    assertTrue( "initial protocol missing from: " + protocolNames.toString(), protocolNames.contains( "jdbc_fr" ) );
    Collection<String> stereotypeNames = schemaCatalog.getStereotypeNames();
    assertTrue( "initial stereotype missing from: " + stereotypeNames.toString(), stereotypeNames.contains( "emps_fr" ) );

    // renaming the schema should retain the tables owned by it.
    catalog( "--schema", AD_HOC_SCHEMA + RENAME_FROM_SUFFIX, "--rename", AD_HOC_SCHEMA + RENAME_TO_SUFFIX );
    tableNames = getSchemaCatalog().getTableNames( "adhoc_to" );
    assertTrue( "renamed table missing from: " + tableNames.toString(), tableNames.contains( "local_fr" ) );

    // renaming a table should not affect stereotypes, formats or protocol associated with it.

    catalog( "--schema", AD_HOC_SCHEMA + RENAME_TO_SUFFIX, "--table", TEST_TABLE_NAME + RENAME_FROM_SUFFIX, "--rename", TEST_TABLE_NAME + RENAME_TO_SUFFIX );
    TableDef testTableDef = schemaCatalog.getSchemaDef( "adhoc_to" ).getTable( "local_to" );
    assertEquals( "renamed table missing stereotype", "emps_fr", testTableDef.getStereotypeName() );

    // renaming a table should not affect formats or protocol associated with it.
    catalog( "--schema", AD_HOC_SCHEMA + RENAME_TO_SUFFIX, "--table", "remote_test", "--rename", "remote_test_rn" );
    TableDef salesTableDef = schemaCatalog.getSchemaDef( "adhoc_to" ).getTable( "remote_test_rn" );
    assertEquals( "renamed table missing format", "table_fr", salesTableDef.getFormat().getName() );
    assertEquals( "renamed table missing protocol", "jdbc_fr", salesTableDef.getProtocol().getName() );

    // renaming a format should not affect the properties
    catalog( "--schema", AD_HOC_SCHEMA + RENAME_TO_SUFFIX, "--format", TABLE_FORMAT_NAME + RENAME_FROM_SUFFIX, "--rename", TABLE_FORMAT_NAME + RENAME_TO_SUFFIX );
    List<String> formatProperties = schemaCatalog.getFormatProperty( "adhoc_to", "table_to", FormatProperties.EXTENSIONS );
    List<String> expectedFormatProperties = Lists.newArrayList( ".jdbc", ".jdbc.lzo" );
    assertTrue( "format properties missing values from " + formatProperties.toString(), formatProperties.containsAll( expectedFormatProperties ) );

    // renaming a protocol should not affect the properties
    catalog( "--schema", AD_HOC_SCHEMA + RENAME_TO_SUFFIX, "--protocol", JDBC_PROTOCOL_NAME + RENAME_FROM_SUFFIX, "--rename", JDBC_PROTOCOL_NAME + RENAME_TO_SUFFIX );
    List<String> protocolProperties = schemaCatalog.getProtocolProperty( "adhoc_to", "jdbc_to", ProtocolProperties.SCHEMES );
    List<String> expectedProtocolProperties = Lists.newArrayList( "jdbc:", "jdbcs:" );
    assertTrue( "protocol properties missing values from " + protocolProperties.toString(), protocolProperties.containsAll( expectedProtocolProperties ) );
    }

  @Test
  public void testShowCommandCLI() throws IOException
    {
    initCatalog();

    catalog( "--schema", "sales", "--add", SALES_SCHEMA );

    catalog(
      "--stereotype", EMPS_STEREOTYPE_NAME, "--add",
      "--columns", Joiner.on( "," ).join( EMPS_COLUMNS ),
      "--types", Joiner.on( "," ).join( EMPS_COLUMN_TYPES )
    );

    catalog( "--schema", AD_HOC_SCHEMA, "--add" );
    catalog( "--schema", AD_HOC_SCHEMA, "--table", TEST_TABLE_NAME, "--add", SALES_EMPS_TABLE, "--stereotype", EMPS_STEREOTYPE_NAME );
    catalog( "--schema", AD_HOC_SCHEMA, "--table" );

    catalog( "--schema", AD_HOC_SCHEMA, "--format", TABLE_FORMAT_NAME, "--add", "--extensions", ".jdbc,.jdbc.lzo", "--provider", "text" );

    catalog( "--schema", AD_HOC_SCHEMA, "--protocol", JDBC_PROTOCOL_NAME, "--add", "--schemes", "jdbc:,jdbcs:", "--provider", "text" );

    catalog( "--schema", AD_HOC_SCHEMA,
      "--table", "remote", "--add", SALES_EMPS_TABLE,
      "--stereotype", EMPS_STEREOTYPE_NAME,
      "--format", "table", "--protocol", "jdbc"
    );

    // test that the various show commands don't fail.
    catalog( "--provider", "text", "--show" );
    catalog( "--schema", AD_HOC_SCHEMA, "--show" );
    catalog( "--schema", AD_HOC_SCHEMA, "--table", TEST_TABLE_NAME, "--show" );
    catalog( "--schema", AD_HOC_SCHEMA, "--format", TABLE_FORMAT_NAME, "--show" );
    catalog( "--schema", AD_HOC_SCHEMA, "--protocol", JDBC_PROTOCOL_NAME, "--show" );

    // commands for invalid targets should fail
    catalog( false, "--provider", "badprovider", "--show" );
    catalog( false, "--schema", AD_HOC_SCHEMA, "--table", "badtable", "--show" );
    catalog( false, "--schema", AD_HOC_SCHEMA, "--format", "badformat", "--show" );
    catalog( false, "--schema", AD_HOC_SCHEMA, "--protocol", "badprotocol", "--show" );
    }
  }
