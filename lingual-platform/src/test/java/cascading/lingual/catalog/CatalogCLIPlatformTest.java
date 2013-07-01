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
  private static final String AD_HOC_SHECMA = "adhoc";

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

    catalog( "--schema", AD_HOC_SHECMA, "--add" );
    catalog( "--schema", AD_HOC_SHECMA, "--table", "local", "--add", SALES_EMPS_TABLE, "--stereotype", "emps"
    );
    catalog( "--schema", AD_HOC_SHECMA, "--table" );
    catalog( "--schema", "ADHOC", "--table" ); // verify case insensitivity

    Collection<String> tableNames = schemaCatalog.getTableNames( AD_HOC_SHECMA );
    assertTrue( AD_HOC_SHECMA + " does not contain table local in " + tableNames.toString(), tableNames.contains( "local" ) );

    catalog( "--schema", AD_HOC_SHECMA, "--format", "table", "--add", "--extensions", ".jdbc,.jdbc.lzo" );

    catalog( "--schema", AD_HOC_SHECMA, "--protocol", "jdbc", "--add", "--uris", "jdbc:,jdbcs:" );

    catalog( "--schema", AD_HOC_SHECMA,
      "--table", "remote", "--add", SALES_EMPS_TABLE,
      "--stereotype", "emps",
      "--format", "table", "--protocol", "jdbc"
    );
    catalog( "--schema", AD_HOC_SHECMA, "--table" );

    catalog( "--schema" );
    }
  }
