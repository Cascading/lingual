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

package cascading.lingual.jdbc.helper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import cascading.lingual.jdbc.JDBCPlatformTestCase;
import org.junit.Test;

/**
 * Test suite that can run SQL statements and check against inline expected
 * result.
 */
public class ProgrammaticSqlTest extends JDBCPlatformTestCase
  {
  protected String getDefaultSchemaPath()
    {
    return SALES_SCHEMA;
    }

  private void assertQueryReturns( String sql, String expected ) throws Exception
    {
    Connection connection = getConnection();
    Statement statement = null;
    ResultSet resultSet = null;
    try
      {
      statement = connection.createStatement();
      resultSet = statement.executeQuery( sql );
      assertEquals( expected, toString( resultSet ) );
      }
    finally
      {
      if( resultSet != null )
        {
        try
          {
          resultSet.close();
          }
        catch( SQLException e )
          {
          // ignore
          }
        }
      if( statement != null )
        {
        try
          {
          statement.close();
          }
        catch( SQLException e )
          {
          // ignore
          }
        }
      }
    }

  protected String toString( ResultSet resultSet ) throws SQLException
    {
    final StringBuilder buf = new StringBuilder();
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    for( int i = 0; i < columnCount; i++ )
      {
      if( i > 0 )
        {
        buf.append( ", " );
        }
      buf.append( "[" ).append( metaData.getColumnName( i + 1 ) ).append( "]" );
      }
    buf.append( "\n" );
    while( resultSet.next() )
      {
      for( int i = 0; i < columnCount; i++ )
        {
        if( i > 0 )
          {
          buf.append( ", " );
          }
        buf.append( resultSet.getObject( i + 1 ) );
        }
      buf.append( "\n" );
      }
    return buf.toString();
    }

  @Test
  public void testFoo() throws Exception
    {
    assertQueryReturns( "select count(*) from sales.depts", "xxx" );
    }
  }
