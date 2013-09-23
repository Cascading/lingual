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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Properties;

import cascading.flow.Flow;
import com.google.common.base.Throwables;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.util.EigenbaseContextException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class LingualStatement implements Statement
  {
  private static final Logger LOG = LoggerFactory.getLogger( LingualStatement.class );

  private final Properties properties;
  private final Statement parent;
  private final LingualConnection lingualConnection;

  private int maxRows;
  private int maxFieldSize;

  public LingualStatement( Properties properties, Statement parent, LingualConnection lingualConnection )
    {
    this.properties = properties;
    this.parent = parent;
    this.lingualConnection = lingualConnection;
    setMaxRows();
    }

  private void setMaxRows()
    {
    if( !properties.contains( Driver.MAX_ROWS ) )
      return;

    try
      {
      setMaxRows( Integer.parseInt( properties.getProperty( Driver.MAX_ROWS ) ) );
      }
    catch( Exception exception )
      {
      throw new RuntimeException( "unable set set max rows", exception );
      }
    }

  @Override
  public void close() throws SQLException
    {
    parent.close();
    }

  @Override
  public int getMaxFieldSize() throws SQLException
    {
    return maxFieldSize;
    }

  @Override
  public void setMaxFieldSize( int max ) throws SQLException
    {
    maxFieldSize = max;
    }

  @Override
  public int getMaxRows() throws SQLException
    {
    return maxRows;
    }

  @Override
  public void setMaxRows( int max ) throws SQLException
    {
    maxRows = max;
    }

  @Override
  public void setEscapeProcessing( boolean enable ) throws SQLException
    {
    parent.setEscapeProcessing( enable );
    }

  @Override
  public int getQueryTimeout() throws SQLException
    {
    return parent.getQueryTimeout();
    }

  @Override
  public void setQueryTimeout( int seconds ) throws SQLException
    {
    parent.setQueryTimeout( seconds );
    }

  @Override
  public void cancel() throws SQLException
    {
    try
      {
      if( !parent.isClosed() )
        parent.cancel();
      }
    finally
      {
      Flow flow = lingualConnection.getCurrentFlow();

      if( flow != null )
        {
        LOG.info( "stopping flow: {}", flow.getID() );
        flow.stop();
        }
      }
    }

  @Override
  public SQLWarning getWarnings() throws SQLException
    {
    return parent.getWarnings();
    }

  @Override
  public void clearWarnings() throws SQLException
    {
    parent.clearWarnings();
    }

  @Override
  public void setCursorName( String name ) throws SQLException
    {
    parent.setCursorName( name );
    }

  @Override
  public boolean execute( String sql ) throws SQLException
    {
    LOG.info( "execute: {}", sql );

    try
      {
      return parent.execute( sql );
      }
    catch( Throwable throwable )
      {
      throw handleThrowable( throwable );
      }
    }

  @Override
  public ResultSet executeQuery( String sql ) throws SQLException
    {
    LOG.info( "executeQuery: {}", sql );

    try
      {
      return parent.executeQuery( sql );
      }
    catch( Throwable throwable )
      {
      throw handleThrowable( throwable );
      }
    }

  @Override
  public int executeUpdate( String sql ) throws SQLException
    {
    LOG.info( "executeUpdate: {}", sql );

    try
      {
      return parent.executeUpdate( sql );
      }
    catch( Throwable throwable )
      {
      throw handleThrowable( throwable );
      }
    }

  private RuntimeException handleThrowable( Throwable throwable ) throws SQLException
    {
    // do not log an OOME
    if( throwable instanceof OutOfMemoryError )
      throw (OutOfMemoryError) throwable;

    LOG.error( "failed with: {}", throwable.getMessage(), throwable );

    if( throwable instanceof SQLException )
      throw (SQLException) throwable;

    if( throwable instanceof EigenbaseContextException )
      {
      String lineMessage = throwable.getMessage();
      String validatorMessage = throwable.getCause() != null ? throwable.getCause().getMessage() : "";

      throw new SQLException( lineMessage + ": \"" + validatorMessage + "\"", throwable );
      }

    if( throwable.getCause() instanceof SqlParseException )
      {
      Throwable cause = throwable.getCause();
      throw new SQLException( cause.getMessage(), cause );
      }

    throw Throwables.propagate( throwable );
    }

  @Override
  public ResultSet getResultSet() throws SQLException
    {
    return parent.getResultSet();
    }

  @Override
  public int getUpdateCount() throws SQLException
    {
    return -1;
    }

  @Override
  public boolean getMoreResults() throws SQLException
    {
    return parent.getMoreResults();
    }

  @Override
  public void setFetchDirection( int direction ) throws SQLException
    {
    parent.setFetchDirection( direction );
    }

  @Override
  public int getFetchDirection() throws SQLException
    {
    return parent.getFetchDirection();
    }

  @Override
  public void setFetchSize( int rows ) throws SQLException
    {
    parent.setFetchSize( rows );
    }

  @Override
  public int getFetchSize() throws SQLException
    {
    return parent.getFetchSize();
    }

  @Override
  public int getResultSetConcurrency() throws SQLException
    {
    return parent.getResultSetConcurrency();
    }

  @Override
  public int getResultSetType() throws SQLException
    {
    return parent.getResultSetType();
    }

  @Override
  public void addBatch( String sql ) throws SQLException
    {
    parent.addBatch( sql );
    }

  @Override
  public void clearBatch() throws SQLException
    {
    parent.clearBatch();
    }

  @Override
  public int[] executeBatch() throws SQLException
    {
    return parent.executeBatch();
    }

  @Override
  public Connection getConnection() throws SQLException
    {
    return parent.getConnection();
    }

  @Override
  public boolean getMoreResults( int current ) throws SQLException
    {
    return parent.getMoreResults( current );
    }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException
    {
    return parent.getGeneratedKeys();
    }

  @Override
  public int executeUpdate( String sql, int autoGeneratedKeys ) throws SQLException
    {
    return parent.executeUpdate( sql, autoGeneratedKeys );
    }

  @Override
  public int executeUpdate( String sql, int[] columnIndexes ) throws SQLException
    {
    return parent.executeUpdate( sql, columnIndexes );
    }

  @Override
  public int executeUpdate( String sql, String[] columnNames ) throws SQLException
    {
    return parent.executeUpdate( sql, columnNames );
    }

  @Override
  public boolean execute( String sql, int autoGeneratedKeys ) throws SQLException
    {
    return parent.execute( sql, autoGeneratedKeys );
    }

  @Override
  public boolean execute( String sql, int[] columnIndexes ) throws SQLException
    {
    return parent.execute( sql, columnIndexes );
    }

  @Override
  public boolean execute( String sql, String[] columnNames ) throws SQLException
    {
    return parent.execute( sql, columnNames );
    }

  @Override
  public int getResultSetHoldability() throws SQLException
    {
    return parent.getResultSetHoldability();
    }

  @Override
  public boolean isClosed() throws SQLException
    {
    return parent.isClosed();
    }

  @Override
  public void setPoolable( boolean poolable ) throws SQLException
    {
    parent.setPoolable( poolable );
    }

  @Override
  public boolean isPoolable() throws SQLException
    {
    return parent.isPoolable();
    }

  public void closeOnCompletion() throws SQLException
    {
    LOG.debug( "This JDK 1.7 feature is not supported" );
    }

  public boolean isCloseOnCompletion() throws SQLException
    {
    LOG.debug( "This JDK 1.7 feature is not supported" );
    return false; // JDK 1.7 feature not supported so tell client to explicitly close.
    }

  @Override
  public <T> T unwrap( Class<T> iface ) throws SQLException
    {
    return parent.unwrap( iface );
    }

  @Override
  public boolean isWrapperFor( Class<?> iface ) throws SQLException
    {
    return parent.isWrapperFor( iface );
    }

  }
