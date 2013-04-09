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

package cascading.lingual.flow;

import java.io.IOException;
import java.lang.reflect.Type;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.lingual.LingualPlatformTestCase;
import cascading.lingual.type.SQLTypeResolver;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;

/**
 *
 */
public class SQLFlowPlatformTest extends LingualPlatformTestCase
  {
  @Test
  public void testStatic() throws IOException
    {
    String statement = "select *\n"
      + "from \"example\".\"sales_fact_1997\" as s\n"
      + "join \"example\".\"employee\" as e\n"
      + "on e.\"EMPID\" = s.\"CUST_ID\"";

    Fields employeeFields = new Fields( "EMPID", "NAME" ).applyTypes( Integer.TYPE, String.class );
    Fields salesFields = new Fields( "CUST_ID", "PROD_ID" ).applyTypes( Integer.TYPE, Integer.TYPE );

    Tap empTap = getPlatform().getDelimitedFile( employeeFields, true, ",", "\"", employeeFields.getTypesClasses(), SIMPLE_EMPLOYEE_TABLE, SinkMode.KEEP );
    Tap salesTap = getPlatform().getDelimitedFile( salesFields, true, ",", "\"", salesFields.getTypesClasses(), SIMPLE_SALES_FACT_TABLE, SinkMode.KEEP );

    Tap resultsTap = getPlatform().getDelimitedFile( Fields.ALL, true, ",", "\"", null, getOutputPath( "static" ), SinkMode.REPLACE );

    FlowDef flowDef = FlowDef.flowDef()
      .setName( "sql flow" )
      .addSource( "employee", empTap )
      .addSource( "sales_fact_1997", salesTap )
      .addSink( "results", resultsTap );

    SQLPlanner sqlPlanner = new SQLPlanner()
      .setDefaultSchema( "example" )
      .setSql( statement );

    flowDef.addAssemblyPlanner( sqlPlanner );

    Flow flow = getPlatform().getFlowConnector().connect( flowDef );

    flow.complete();

    validateLength( flow, 2 );
    }

  @Test
  public void testDynamic() throws IOException
    {
    String statement = "select *\n"
      + "from \"example\".\"sales_fact_1997\" as s\n"
      + "join \"example\".\"employee\" as e\n"
      + "on e.\"EMPID\" = s.\"CUST_ID\"";

    Tap empTap = getPlatform().getDelimitedFile( ",", "\"", new SQLTypeResolver(), SIMPLE_EMPLOYEE_TABLE, SinkMode.KEEP );
    Tap salesTap = getPlatform().getDelimitedFile( ",", "\"", new SQLTypeResolver(), SIMPLE_SALES_FACT_TABLE, SinkMode.KEEP );

    Tap resultsTap = getPlatform().getDelimitedFile( ",", "\"", new SQLTypeResolver(), getOutputPath( "dynamic" ), SinkMode.REPLACE );

    FlowDef flowDef = FlowDef.flowDef()
      .setName( "sql flow" )
      .addSource( "employee", empTap )
      .addSource( "sales_fact_1997", salesTap )
      .addSink( "results", resultsTap );

    SQLPlanner sqlPlanner = new SQLPlanner()
      .setDefaultSchema( "example" )
      .setSql( statement );

    flowDef.addAssemblyPlanner( sqlPlanner );

    Flow flow = getPlatform().getFlowConnector().connect( flowDef );

    flow.complete();

    validateLength( flow, 2 );
    }

  @Test
  public void testDynamicBindSchema() throws IOException
    {
    String statement = "select *\n"
      + "from \"example\".\"sales_fact_1997\" as s\n"
      + "join \"example\".\"employee\" as e\n"
      + "on e.\"EMPID\" = s.\"CUST_ID\"";

    Tap empTap = getPlatform().getDelimitedFile( ",", "\"", new SQLTypeResolver(), SIMPLE_EMPLOYEE_TABLE, SinkMode.KEEP );
    Tap salesTap = getPlatform().getDelimitedFile( ",", "\"", new SQLTypeResolver(), SIMPLE_SALES_FACT_TABLE, SinkMode.KEEP );

    Tap resultsTap = getPlatform().getDelimitedFile( ",", "\"", new SQLTypeResolver(), getOutputPath( "bindschema" ), SinkMode.REPLACE );

    FlowDef flowDef = FlowDef.flowDef()
      .setName( "sql flow" )
      .addSource( "example.employee", empTap )
      .addSource( "example.sales_fact_1997", salesTap )
      .addSink( "results", resultsTap );

    SQLPlanner sqlPlanner = new SQLPlanner()
      .setSql( statement );

    flowDef.addAssemblyPlanner( sqlPlanner );

    Flow flow = getPlatform().getFlowConnector().connect( flowDef );

    flow.complete();

    validateLength( flow, 2 );
    }

  @Test
  public void testInsert() throws IOException
    {
    String statement = "insert into \"results\" select *\n"
      + "from \"example\".\"sales_fact_1997\" as s\n"
      + "join \"example\".\"employee\" as e\n"
      + "on e.\"EMPID\" = s.\"CUST_ID\"";

    Tap empTap = getPlatform().getDelimitedFile( ",", "\"", new SQLTypeResolver(), SIMPLE_EMPLOYEE_TABLE, SinkMode.KEEP );
    Tap salesTap = getPlatform().getDelimitedFile( ",", "\"", new SQLTypeResolver(), SIMPLE_SALES_FACT_TABLE, SinkMode.KEEP );

    Comparable[] names = {"CUST_ID", "PROD_ID", "EMP_ID", "NAME"};
    Type[] types = {Integer.class, Integer.class, Integer.class, String.class};
    Fields sinkFields = new Fields( names, types );
    Tap resultsTap = getPlatform().getDelimitedFile( sinkFields, true, ",", "\"", null, getOutputPath( "insert" ), SinkMode.REPLACE );

    FlowDef flowDef = FlowDef.flowDef()
      .setName( "sql flow" )
      .addSource( "example.employee", empTap )
      .addSource( "example.sales_fact_1997", salesTap )
      .addSink( "results", resultsTap );

    SQLPlanner sqlPlanner = new SQLPlanner()
      .setSql( statement );

    flowDef.addAssemblyPlanner( sqlPlanner );

    Flow flow = getPlatform().getFlowConnector().connect( flowDef );

    flow.complete();

    validateLength( flow, 2 );
    }
  }
