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

import java.io.IOException;
import java.util.Properties;

import cascading.lingual.LingualPlatformTestCase;
import cascading.lingual.common.Main;
import cascading.lingual.jdbc.Driver;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.platform.PlatformBrokerFactory;
import com.google.common.base.Joiner;
import com.google.common.collect.ObjectArrays;
import org.junit.Test;

/**
 *
 */
public class CatalogCLIPlatformTestCase extends LingualPlatformTestCase
  {
  @Override
  public void setUp() throws Exception
    {
    super.setUp();
    }

  @Test
  public void testCLI() throws IOException
    {
    String outputPath = getOutputPath( "catalogcli" );

    getPlatform().remoteRemove( outputPath, true );

    Main.setLogLevel( "debug" );

    execute( outputPath, "--init" );
    execute( outputPath, "--schema", "sales", "--add", SALES_SCHEMA );

    execute( outputPath, "--stereotype", "emps", "--add",
      "--columns", Joiner.on( "," ).join( EMPS_COLUMNS ),
      "--types", Joiner.on( "," ).join( EMPS_COLUMN_TYPES )
    );

    execute( outputPath, "--schema", "adhoc", "--table", "local", "--add", EMPS_TABLE,
      "--stereotype", "emps"
    );
    execute( outputPath, "--schema", "adhoc", "--table" );

    execute( outputPath, "--schema", "adhoc", "--format", "table", "--add", "--extensions", ".jdbc,.jdbc.lzo" );
    execute( outputPath, "--schema", "adhoc", "--protocol", "jdbc", "--add", "--uris", "jdbc:,jdbcs:" );

    execute( outputPath, "--schema", "adhoc", "--table", "remote", "--add", EMPS_TABLE,
      "--stereotype", "emps",
      "--format", "table", "--protocol", "jdbc"
    );
    execute( outputPath, "--schema", "adhoc", "--table" );

    execute( outputPath, "--schema" );
    }

  private void execute( String testName, String... args ) throws IOException
    {
    args = ObjectArrays.concat( new String[]{"--verbose", "debug"}, args, String.class );

    assertTrue( "execute returned false", createCatalog( testName ).execute( args ) );
    }

  private Catalog createCatalog( String rootPath )
    {
    Properties properties = new Properties();

    properties.setProperty( Driver.CATALOG_PROP, rootPath );
    properties.setProperty( PlatformBroker.META_DATA_PATH_PROP, "_lingual" );
    properties.setProperty( PlatformBroker.CATALOG_FILE_PROP, "catalog.ser" );
    properties.setProperty( PlatformBrokerFactory.PLATFORM_NAME, getPlatformName() );

    return new Catalog( properties );
    }
  }
