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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/** A trivial JDBC Example */
public class JdbcExample
  {
  public static void main( String[] args ) throws Exception
    {
    new JdbcExample().run();
    }

  public void run() throws ClassNotFoundException, SQLException
    {
    /*
    The roughly equivalent Cascading code..

    Tap empTap = new FileTap(new TextDelimited(true, ",", "\""), "src/test/data/employee.txt");
    Tap salesTap = new FileTap(new TextDelimited(true, ",", "\""), "src/test/data/salesfact.txt");

    Tap resultsTap = new FileTap(new TextDelimited(true, ",", "\""),
      "build/test/output/results.txt", SinkMode.REPLACE);

    Pipe empPipe = new Pipe("emp");
    Pipe salesPipe = new Pipe("sales");

    Pipe join = new CoGroup(empPipe, new Fields("empid"), salesPipe, new Fields("cust_id"));

    FlowDef flowDef = flowDef()
            .setName("flow")
            .addSource(empPipe, empTap)
            .addSource(salesPipe, salesTap)
            .addTailSink(join, resultsTap);

    Flow flow = new LocalFlowConnector().connect(flowDef);

    flow.start();

    TupleEntryIterator iterator = resultTap.openForRead();
    */

    Class.forName( "cascading.lingual.jdbc.Driver" );
    Connection connection = DriverManager.getConnection(
      "jdbc:lingual:local;schemas=src/main/resources/data/example" );

    Statement statement = connection.createStatement();

    ResultSet resultSet =
      statement.executeQuery(
        "SELECT *\n"
          + "FROM \"example\".\"sales_fact_1997\" AS s\n"
          + "JOIN \"example\".\"employee\" AS e\n"
          + "ON e.\"EMPID\" = s.\"CUST_ID\"" );

    while( resultSet.next() )
      {
      int n = resultSet.getMetaData().getColumnCount();
      StringBuilder builder = new StringBuilder();

      for( int i = 1; i <= n; i++ )
        {
        builder.append(
          ( i > 1 ? "; " : "" )
            + resultSet.getMetaData().getColumnLabel( i )
            + "="
            + resultSet.getObject( i ) );
        }
      System.out.println( builder );
      }

    resultSet.close();
    statement.close();
    connection.close();
    }
  }
