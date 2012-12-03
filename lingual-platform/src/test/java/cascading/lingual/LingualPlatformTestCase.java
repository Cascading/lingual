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

package cascading.lingual;

import cascading.PlatformTestCase;
import cascading.tuple.Fields;

/**
 *
 */
public class LingualPlatformTestCase extends PlatformTestCase
  {
  public static final String DATA_PATH = System.getProperty( "test.data.path", "../lingual-platform/src/test/resources/data/" );
  public static final String SALES_SCHEMA_NAME = "sales";
  public static final String SALES_SCHEMA = DATA_PATH + SALES_SCHEMA_NAME + "/";
  public static final String DEPTS_TABLE = SALES_SCHEMA + "depts.tcsv";
  public static final String EMPS_TABLE = SALES_SCHEMA + "emps.tcsv";

  public static final String[] EMPS_COLUMNS = new String[]{
    "EMPNO", "NAME", "DEPTNO", "GENDER", "CITY", "EMPID", "AGE", "SLACKER", "MANAGER"
  };

  public static String[] EMPS_COLUMN_TYPES = new String[]{
    "int", "string", "int", "string", "string", "int", "int", "boolean", "boolean"
  };

  public static Class[] EMPS_TYPES = new Class[]{
    int.class, String.class, int.class, String.class, String.class, int.class, int.class, boolean.class, boolean.class
  };

  public static Fields EMPS_FIELDS = new Fields( EMPS_COLUMNS );

  }
