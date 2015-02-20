/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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
public class DefaultProviderPlatformTest extends CLIPlatformTestCase
  {

  private final String SIMPLE_STEREOTYPE = "keyval";

  public final String PROVIDER_SCHEMA = DATA_PATH + "/" + "provider";

  @Test
  public void testCsvRead() throws IOException
    {
    runFileTest( "simple_csv_valid.csv" );
    }

  @Test
  public void testTcsvRead() throws IOException
    {
    runFileTest( "simple_tcsv_valid.tcsv" );
    }

  @Test
  public void testTsvRead() throws IOException
    {
    runFileTest( "simple_tsv_valid.tsv" );
    }

  @Test
  public void testTtsvRead() throws IOException
    {
    runFileTest( "simple_ttsv_valid.ttsv" );
    }

  @Test
  public void testNoHeaderRead() throws IOException
    {

    String fileName = "no_header_csv.ncsv";

    copyFromLocal( PROVIDER_SCHEMA + "/" + fileName );

    initCatalog();

    catalog( "--schema", EXAMPLE_SCHEMA, "--add", getSchemaPath( EXAMPLE_SCHEMA ) );

    catalog( "--schema", EXAMPLE_SCHEMA, "--stereotype", SIMPLE_STEREOTYPE, "--add", "--columns", "name, number", "--types", "int,int" );

    String tableName = fileName.split( "\\." )[ 0 ];

    catalog( "--schema", EXAMPLE_SCHEMA,
      "--format", "csv", "--add", "--extensions", ".ncsv", "--provider", "text",
      "--properties", "header=false"
    );

    catalog( "--schema", EXAMPLE_SCHEMA, "--table", tableName, "--stereotype", SIMPLE_STEREOTYPE, "--add", PROVIDER_SCHEMA + "/" + fileName );

    shellSQL( "select * from \"example\".\"" + tableName + "\";" );
    }

  protected void runFileTest( String fileName ) throws IOException
    {
    copyFromLocal( PROVIDER_SCHEMA + "/" + fileName );

    initCatalog();

    catalog( "--schema", EXAMPLE_SCHEMA, "--add", getSchemaPath( EXAMPLE_SCHEMA ) );

    catalog( "--schema", EXAMPLE_SCHEMA, "--stereotype", SIMPLE_STEREOTYPE, "--add", "--columns", "name, number", "--types", "int,int" );

    String tableName = fileName.split( "\\." )[ 0 ];

    catalog( "--schema", EXAMPLE_SCHEMA, "--table", tableName, "--stereotype", SIMPLE_STEREOTYPE, "--add", PROVIDER_SCHEMA + "/" + fileName );

    shellSQL( "select * from \"example\".\"" + tableName + "\";" );
    }


  }
