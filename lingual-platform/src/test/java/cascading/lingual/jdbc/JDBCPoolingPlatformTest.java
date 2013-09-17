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

import java.beans.PropertyVetoException;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Time;

import cascading.lingual.optiq.FieldTypeFactory;
import cascading.lingual.platform.PlatformBrokerFactory;
import cascading.lingual.type.SQLDateTimeCoercibleType;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.type.CoercibleType;
import com.google.common.collect.Table;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.ConnectionCustomizer;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import org.eigenbase.sql.type.BasicSqlType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCPoolingPlatformTest extends JDBCPlatformTestCase
  {
  public static final String URI = "jdbc:lingual";
  public static final String DRIVER_CLASSNAME = cascading.lingual.jdbc.Driver.class.getName();
  private static final int POOL_SIZE = 5;

  public static ComboPooledDataSource comboPooledDataSource = null;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Override
  public void setUp() throws Exception
    {
    super.setUp();
    PlatformBrokerFactory.instance().reloadBrokers();
    // leave enabled for now
    enableLogging( "cascading.lingual", "debug" );

    // all tests share pool to make detecting leaks easier.
    if( comboPooledDataSource == null )
      {
      comboPooledDataSource = new ComboPooledDataSource();
      try
        {
        comboPooledDataSource.setDriverClass( DRIVER_CLASSNAME );
        }
      catch( PropertyVetoException exception )
        {
        exception.printStackTrace();
        }
      comboPooledDataSource.setConnectionCustomizerClassName( TestingConnectionCustomizer.class.getName() );
      comboPooledDataSource.setJdbcUrl( getConnectionString() );
      comboPooledDataSource.setMinPoolSize( POOL_SIZE );
      comboPooledDataSource.setMaxPoolSize( POOL_SIZE );
      // the concept of a connection-cached DB statement doesn't apply to Lingual
      // but make sure that having it doesn't cause issues.
      comboPooledDataSource.setMaxStatements( POOL_SIZE );
      }
    }

  @Override
  public void tearDown() throws Exception
    {
    super.tearDown();
    }

  @Override
  protected String getDefaultSchemaPath()
    {
    return SALES_SCHEMA;
    }

  @Test
  public void runQueriesWithProperConnectionClosing() throws Exception
    {
    // run more queries than connections available in the pool to confirm that closing connections returns them to the pool.
    assertTablesEqual( "emps-select", "select empno, name from sales.emps", true, true );
    assertTablesEqual( "emps-filter-one", "select name from sales.emps where empno = 120", true, true );
    assertTablesEqual( "emps-filter-one", "select name from sales.emps where name = 'Wilma'", true, true );
    assertTablesEqual( "emps-filter-one", "select name from sales.emps where name like 'W%ma'", true, true );
    assertTablesEqual( "emps-select", "select empno, name from sales.emps", true, true );
    assertTablesEqual( "emps-filter-one", "select name from sales.emps where empno = 120", true, true );
    assertTablesEqual( "emps-filter-one", "select name from sales.emps where name = 'Wilma'", true, true );
    assertTablesEqual( "emps-filter-one", "select name from sales.emps where name like 'W%ma'", true, true );
    }

  @Test
  public void runQueriesWithNoConnectionClosing() throws Exception
    {
    assertTablesEqual( "emps-select", "select empno, name from sales.emps", false, true );
    assertTablesEqual( "emps-filter-one", "select name from sales.emps where empno = 120", false, true );
    assertTablesEqual( "emps-filter-one", "select name from sales.emps where name = 'Wilma'", false, true );
    assertTablesEqual( "emps-filter-one", "select name from sales.emps where name like 'W%ma'", false, true );

    // with four connections tied up we should be able to get only one more connection
    Connection connection5 = getConnection( true );
    assertNotNull( "failed to get connection", connection5 );
    Connection connection6 = getConnection( false );
    assertNull( "should not have gotten connection", connection6 );
    }

  protected void assertTablesEqual( String tableName, String sqlQuery, boolean closeConnection, boolean expectSuccess ) throws Exception
    {
    TupleEntryIterator entryIterator = getTable( tableName );
    Table expectedTable = createTable( entryIterator );

    assertTablesEqual( expectedTable, sqlQuery, closeConnection, expectSuccess );
    }


  protected void assertTablesEqual( Table expectedTable, String sqlQuery, boolean closeConnection, boolean expectSuccess ) throws Exception
    {
    Connection connection = getConnection( expectSuccess );

    try
      {
      ResultSet result = executeSql( sqlQuery, connection );
      Table resultTable = createTable( result );
      result.close();
      assertEquals( expectedTable, resultTable );
      }
    finally
      {
      if( closeConnection )
        connection.close();
      }
    }

  protected ResultSet executeSql( String sql, Connection connection ) throws Exception
    {
    return connection.createStatement().executeQuery( sql );
    }

  protected synchronized Connection getConnection( boolean expectSuccess ) throws Exception
    {
    String stats = "Max connections: " + comboPooledDataSource.getMaxPoolSize()
      + " Idle: " + comboPooledDataSource.getNumIdleConnections()
      + " Busy: " + comboPooledDataSource.getNumBusyConnections()
      + " Unclosed orphans: " + comboPooledDataSource.getNumUnclosedOrphanedConnections();
    int connectionsAllocated = comboPooledDataSource.getNumBusyConnections() + comboPooledDataSource.getNumUnclosedOrphanedConnections();

    if( expectSuccess )
      {
      assertTrue( "connection pool is maxed out: " + stats, connectionsAllocated < comboPooledDataSource.getMaxPoolSize() );
      return comboPooledDataSource.getConnection();
      }
    else
      {
      assertTrue( "connection pool is not maxed out: " + stats, connectionsAllocated == comboPooledDataSource.getMaxPoolSize() );
      return null;
      }
    }

  @Override
  protected Table<Integer, Comparable, Object> createTable( TupleEntryIterator entryIterator, boolean useOrdinal )
    {
    Table<Integer, Comparable, Object> table = createNullableTable();

    JavaTypeFactory typeFactory = new FieldTypeFactory();
    int row = 0;
    while( entryIterator.hasNext() )
      {
      TupleEntry entry = entryIterator.next();

      for( Comparable field : entry.getFields() )
        {
        // we must coerce into the actual sql type returned by the result-set
        Object value = entry.getObject( field );
        int columnPos = entry.getFields().getPos( field );
        Type type = entry.getFields().getType( columnPos );

        if( type instanceof BasicSqlType )
          {
          value = ( (CoercibleType) type ).coerce( value, typeFactory.getJavaClass( ( (BasicSqlType) type ) ) );

          // for date-time types, the canonical type (int or long) -- chosen for
          // efficient internal processing -- is not what is returned to the
          // end-user from JDBC (java.sql.Date etc.)
          switch( ( (BasicSqlType) type ).getSqlTypeName() )
            {
            case DATE:
              value = new java.sql.Date( ( (Integer) value ).longValue() * SQLDateTimeCoercibleType.MILLIS_PER_DAY );
              break;
            case TIME:
              value = new Time( ( (Integer) value ).longValue() );
              break;
            case TIMESTAMP:
              value = new java.sql.Date( ( (Long) value ).longValue() );
              break;
            }
          }

        if( useOrdinal )
          field = columnPos;

        if( value != null )
          table.put( row++, field, value );
        }
      }

    return table;
    }

  /** Logs connection pool actions */
  public static class TestingConnectionCustomizer implements ConnectionCustomizer
    {
    private static final Logger LOG = LoggerFactory.getLogger( TestingConnectionCustomizer.class );

    public TestingConnectionCustomizer()
      {
      }

    @Override
    public void onAcquire( Connection connection, String parentDataSourceIdentityToken ) throws Exception
      {
      logEvent( "Acquired", connection, parentDataSourceIdentityToken );
      }

    @Override
    public void onDestroy( Connection connection, String parentDataSourceIdentityToken ) throws Exception
      {
      logEvent( "Destroyed", connection, parentDataSourceIdentityToken );
      }

    @Override
    public void onCheckOut( Connection connection, String parentDataSourceIdentityToken ) throws Exception
      {
      logEvent( "CheckedOut", connection, parentDataSourceIdentityToken );
      }

    @Override
    public void onCheckIn( Connection connection, String parentDataSourceIdentityToken ) throws Exception
      {
      logEvent( "CheckedIn", connection, parentDataSourceIdentityToken );
      }

    public void logEvent( String eventName, Connection connection, String parentDataSourceIdentityToken )
      {
      LOG.debug( eventName + " " + connection + " [" + parentDataSourceIdentityToken + "]" );
      }
    }
  }
