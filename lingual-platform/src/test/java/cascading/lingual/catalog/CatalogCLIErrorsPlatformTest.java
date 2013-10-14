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

/**
 *
 */
public class CatalogCLIErrorsPlatformTest extends CatalogCLIPlatformTest
  {

  public void noStereotypeForTable() throws IOException
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


    catalog( "--schema", AD_HOC_SCHEMA, "--add", getSchemaPath( AD_HOC_SCHEMA ) );
    catalog( "--schema", AD_HOC_SCHEMA, "--table", TEST_TABLE_NAME, "--add", SALES_EMPS_TABLE, "--stereotype", EMPS_STEREOTYPE_NAME );
    catalog( "--schema", AD_HOC_SCHEMA, "--table" );

    Collection<String> tableNames = schemaCatalog.getTableNames( AD_HOC_SCHEMA );
    assertTrue( AD_HOC_SCHEMA + " does not contain table " + TEST_TABLE_NAME + " in " + tableNames.toString(), tableNames.contains( TEST_TABLE_NAME ) );

    //without a provider, jdbc:oracle isn't valid so this should fail
    catalog( false, "--schema", AD_HOC_SCHEMA, "--table", TEST_TABLE_NAME, "--add", SALES_EMPS_TABLE, "--stereotype", EMPS_STEREOTYPE_NAME,
      "jdbc:oracle:thin:hr/hr@localhost:1521:XE" );
    }

  }
