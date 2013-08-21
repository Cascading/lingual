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

package cascading.lingual;

import cascading.PlatformTestCase;
import org.apache.log4j.Level;

/**
 *
 */
public abstract class LingualPlatformTestCase extends PlatformTestCase
  {
  public static final String DATA_PATH = System.getProperty( "test.data.path", "../lingual-platform/src/test/resources/data/" );
  public static final String PROVIDER_PATH = System.getProperty( "test.providerjar.path", "../lingual-platform/src/test/resources/provider/" );
  public static final String QUERY_FILES_PATH = System.getProperty( "test.queryfiles.path", "../lingual-platform/src/test/resources/query/" );

  // NB: while catalog schema/table definitions handle case normalization all these files and schemas
  // need to match case to prevent unit test flapping under OS X's "ignore case but retain it" design.
  public static final String SALES_SCHEMA_NAME = "SALES";
  public static final String SALES_SCHEMA = DATA_PATH + SALES_SCHEMA_NAME + "/";
  public static final String SALES_DEPTS_TABLE = SALES_SCHEMA + "DEPTS.tcsv";
  public static final String SALES_EMPS_TABLE = SALES_SCHEMA + "EMPS.tcsv";
  public static final String SALES_SALES_TABLE = SALES_SCHEMA + "SALES.tcsv";

  public static final String SIMPLE_SCHEMA_NAME = "simple";
  public static final String SIMPLE_SCHEMA = DATA_PATH + SIMPLE_SCHEMA_NAME + "/";
  public static final String SIMPLE_EMPLOYEE_TABLE = SIMPLE_SCHEMA + "employee.tcsv";
  public static final String SIMPLE_SALES_FACT_TABLE = SIMPLE_SCHEMA + "sales_fact_1997.tcsv";
  public static final String SIMPLE_PRODUCTS_TABLE = SIMPLE_SCHEMA + "products.tpsv";

  public static final String EXAMPLE_SCHEMA = "example";


  public static final String[] EMPS_COLUMNS = new String[]{
    "EMPNO", "NAME", "DEPTNO", "GENDER", "CITY", "EMPID", "AGE", "SLACKER", "MANAGER"
  };

  public static final String[] EMPS_COLUMN_TYPES = new String[]{
    "int", "string", "int", "string", "string", "int", "int", "boolean", "boolean"
  };

  public static final String[] PRODUCTS_COLUMNS = new String[]{
    "SKU", "PRODUCT_NAME", "PRODUCT_DESCRIPTION"
  };

  public static final String[] PRODUCTS_COLUMN_TYPES = new String[]{
    "string", "string", "string"
  };

  private String resultPath;

  protected LingualPlatformTestCase( boolean useCluster )
    {
    super( useCluster );
    }

  public LingualPlatformTestCase()
    {
    }

  public static void enableLogging( String log, String level )
    {
    org.apache.log4j.Logger.getLogger( log ).setLevel( Level.toLevel( level.toUpperCase() ) );
    }

  protected String getResultPath()
    {
    if( resultPath == null )
      resultPath = getOutputPath( "results/" + getTestName() );

    return resultPath;
    }

  protected String getCatalogPath()
    {
    return getRootPath() + "/catalog/" + getTestName();
    }

  protected String getFlowPlanPath()
    {
    return getRootPath() + "/dot/" + getTestName();
    }

  protected String getSQLPlanPath()
    {
    return getRootPath() + "/optiq/" + getTestName();
    }
  }
