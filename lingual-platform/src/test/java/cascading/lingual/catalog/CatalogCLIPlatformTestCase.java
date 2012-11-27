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
import cascading.lingual.jdbc.Driver;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.platform.PlatformBrokerFactory;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.google.common.base.Joiner;
import org.junit.Test;

/**
 *
 */
public class CatalogCLIPlatformTestCase extends LingualPlatformTestCase
  {
  private String tapClass;
  private String schemeClass;

  @Override
  public void setUp() throws Exception
    {
    super.setUp();

    Tap tap = getPlatform().getDelimitedFile( Fields.UNKNOWN, ",", "foo" );

    tapClass = tap.getClass().getName();
    schemeClass = tap.getScheme().getClass().getName();
    }

  @Test
  public void testCLI() throws IOException
    {
    String outputPath = getOutputPath( "catalogcli" );

    getPlatform().remoteRemove( outputPath, true );

    execute( outputPath, "--init" );
    execute( outputPath, "--schema", "sales", "--add", SALES_SCHEMA );

//    execute( outputPath, "--format", "csv", "--add", "--exts", ".csv",
//      "--scheme", schemeClass,
//      "--scheme-param", "hasHeader=true,delimiter=\\,,quote=\""
//    );
//
//    execute( outputPath, "--protocol", "http", "--add", "--uris", "http:,https:",
//      "--tap", tapClass,
//      "--tap-param", "..."
//    );

    execute( outputPath, "--stereotype", "emps", "--add",
      "--columns", Joiner.on( "," ).join( EMPS_COLUMNS ),
      "--types", Joiner.on( "," ).join( EMPS_COLUMN_TYPES ),
      "--formats", "csv",
      "--protocols", "http"
    );

    execute( outputPath, "--schema", "adhoc", "--table", "emps", "--add", EMPS_TABLE,
      "--stereotype", "emps"
    );
    execute( outputPath, "--schema", "adhoc", "--table" );
    execute( outputPath, "--schema" );

    }

  private void execute( String testName, String... args ) throws IOException
    {
    createCatalog( testName ).execute( args );
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
