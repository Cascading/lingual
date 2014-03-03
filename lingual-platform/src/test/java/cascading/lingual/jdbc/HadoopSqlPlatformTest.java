/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.lingual.jdbc;

import org.junit.Test;

/**
 */
public class HadoopSqlPlatformTest extends JDBCPlatformTestCase
  {
  public static final String SALES_SCHEMA_DIRECTORY = DATA_PATH + "sales2" + "/";

  @Override
  protected String getDefaultSchemaPath()
    {
    return SALES_SCHEMA_DIRECTORY;
    }

//  protected String getPlannerDebug()
//    {
//    return DebugLevel.VERBOSE.toString();
//    }

  @Test
  public void testSelectFromSubDirs() throws Exception
    {
    if( !getPlatform().isMapReduce() )
      return;

    assertTablesEqual( "sales-select-date", "select empno, sale_date, sale_time from \"sales2\".\"sales\" where sale_date > date'1993-01-01' order by empno" );
    }
  }
