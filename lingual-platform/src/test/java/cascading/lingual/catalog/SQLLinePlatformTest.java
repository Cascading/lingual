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

import org.junit.Test;

/**
 *
 */
public class SQLLinePlatformTest extends CLIPlatformTestCase
  {
  @Test
  public void testGoodQueryReturnsTrue() throws IOException
    {
    copyFromLocal( SIMPLE_EMPLOYEE_TABLE );

    initCatalog();

    catalog( "--schema", EXAMPLE_SCHEMA, "--add", getSchemaPath( EXAMPLE_SCHEMA ) );

    catalog( "--schema", EXAMPLE_SCHEMA, "--table", "employee", "--add", SIMPLE_EMPLOYEE_TABLE );

    shellSQL( true, "select * from \"example\".\"employee\";" );
    shellSQL( "select * from \"example\".\"employee\";" );
    }

  @Test
  public void testBadQueryReturnsFalse() throws IOException
    {
    copyFromLocal( SIMPLE_PRODUCTS_TABLE );
    initCatalog();
    shellSQL( false, "THIS IS NOT A VALID SQL COMMAND AND SHOULD FAIL" );
    }

  @Test
  public void testBadTableReturnsFalse() throws IOException
    {
    copyFromLocal( SIMPLE_PRODUCTS_TABLE );
    initCatalog();
    shellSQL( false, "select * from \"example\".\"oops\";" );
    }
  }
