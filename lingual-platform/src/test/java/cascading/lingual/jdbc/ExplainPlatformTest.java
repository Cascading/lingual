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

package cascading.lingual.jdbc;

import java.sql.ResultSet;

import org.junit.Test;

/** Tests that statements are executed using a particular plan. */
public class ExplainPlatformTest extends JDBCPlatformTestCase
  {
  @Override
  protected String getDefaultSchemaPath()
    {
    return SALES_SCHEMA;
    }

  @Test
  public void testPureSelectUsesTap() throws Exception
    {
    assertPlan2( "select * from sales.emps",
      "EnumerableCalcRel(expr#0..9=[{inputs}], proj#0..9=[{exprs}])\n"
        + "  EnumerableTapRel(table=[[SALES, EMPS]])\n" );
    }

  @Test
  public void testSelectWhereDoesNotUseTap() throws Exception
    {
    assertPlan2(
      "select * from sales.emps where empno = 1",
      "CascadingEnumerableRel\n"
        + "  CascadingCalcRel(expr#0..9=[{inputs}], expr#10=[CAST($t0):INTEGER NOT NULL], expr#11=[1], expr#12=[=($t10, $t11)], proj#0..9=[{exprs}], $condition=[$t12])\n"
        + "    CascadingTableAccessRel(table=[[SALES, EMPS]])\n" );
    }

  private void assertPlan2( String sql, String expectedPlan ) throws Exception
    {
    assertEquals( expectedPlan, getPlan( sql ) );
    }

  private String getPlan( String sql ) throws Exception
    {
    ResultSet result = executeSql( "explain plan for " + sql );
    assertTrue( result.next() );
    assertEquals( 1, result.getMetaData().getColumnCount() );
    final String plan = result.getString( 1 );
    assertFalse( result.next() );
    result.close();
    return plan;
    }
  }
