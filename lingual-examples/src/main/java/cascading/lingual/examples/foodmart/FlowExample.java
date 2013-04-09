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

package cascading.lingual.examples.foodmart;

import java.io.IOException;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.lingual.flow.SQLPlanner;
import cascading.lingual.tap.local.SQLTypedTextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.TupleEntryIterator;

/**
 *
 */
public class FlowExample
  {
  public static void main( String[] args ) throws Exception
    {
    new FlowExample().run();
    }

  public void run() throws IOException
    {
    String statement = "select *\n"
      + "from \"example\".\"sales_fact_1997\" as s\n"
      + "join \"example\".\"employee\" as e\n"
      + "on e.\"EMPID\" = s.\"CUST_ID\"";

    Tap empTap = new FileTap( new SQLTypedTextDelimited( ",", "\"" ), "src/main/resources/data/example/employee.tcsv", SinkMode.KEEP );
    Tap salesTap = new FileTap( new SQLTypedTextDelimited( ",", "\"" ), "src/main/resources/data/example/sales_fact_1997.tcsv", SinkMode.KEEP );

    Tap resultsTap = new FileTap( new SQLTypedTextDelimited( ",", "\"" ), "build/test/output/flow/results.tcsv", SinkMode.REPLACE );

    FlowDef flowDef = FlowDef.flowDef()
      .setName( "sql flow" )
      .addSource( "example.employee", empTap )
      .addSource( "example.sales_fact_1997", salesTap )
      .addSink( "results", resultsTap );

    SQLPlanner sqlPlanner = new SQLPlanner()
      .setSql( statement );

    flowDef.addAssemblyPlanner( sqlPlanner );

    Flow flow = new LocalFlowConnector().connect( flowDef );

    flow.complete();

    TupleEntryIterator iterator = resultsTap.openForRead( flow.getFlowProcess() );

    while( iterator.hasNext() )
      System.out.println( iterator.next() );

    iterator.close();
    }
  }
