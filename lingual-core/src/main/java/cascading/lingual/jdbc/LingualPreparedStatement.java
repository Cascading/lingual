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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Properties;

/**
 *
 */
class LingualPreparedStatement extends LingualStatement implements PreparedStatement
  {
  private final PreparedStatement parent;

  public LingualPreparedStatement( Properties properties, PreparedStatement parent, LingualConnection lingualConnection )
    {
    super( properties, parent, lingualConnection );
    this.parent = parent;
    }

  @Override
  public ResultSet executeQuery() throws SQLException
    {
    return parent.executeQuery();
    }

  @Override
  public int executeUpdate() throws SQLException
    {
    return parent.executeUpdate();
    }

  @Override
  public void setNull( int parameterIndex, int sqlType ) throws SQLException
    {
    parent.setNull( parameterIndex, sqlType );
    }

  @Override
  public void setBoolean( int parameterIndex, boolean x ) throws SQLException
    {
    parent.setBoolean( parameterIndex, x );
    }

  @Override
  public void setByte( int parameterIndex, byte x ) throws SQLException
    {
    parent.setByte( parameterIndex, x );
    }

  @Override
  public void setShort( int parameterIndex, short x ) throws SQLException
    {
    parent.setShort( parameterIndex, x );
    }

  @Override
  public void setInt( int parameterIndex, int x ) throws SQLException
    {
    parent.setInt( parameterIndex, x );
    }

  @Override
  public void setLong( int parameterIndex, long x ) throws SQLException
    {
    parent.setLong( parameterIndex, x );
    }

  @Override
  public void setFloat( int parameterIndex, float x ) throws SQLException
    {
    parent.setFloat( parameterIndex, x );
    }

  @Override
  public void setDouble( int parameterIndex, double x ) throws SQLException
    {
    parent.setDouble( parameterIndex, x );
    }

  @Override
  public void setBigDecimal( int parameterIndex, BigDecimal x ) throws SQLException
    {
    parent.setBigDecimal( parameterIndex, x );
    }

  @Override
  public void setString( int parameterIndex, String x ) throws SQLException
    {
    parent.setString( parameterIndex, x );
    }

  @Override
  public void setBytes( int parameterIndex, byte[] x ) throws SQLException
    {
    parent.setBytes( parameterIndex, x );
    }

  @Override
  public void setDate( int parameterIndex, Date x ) throws SQLException
    {
    parent.setDate( parameterIndex, x );
    }

  @Override
  public void setTime( int parameterIndex, Time x ) throws SQLException
    {
    parent.setTime( parameterIndex, x );
    }

  @Override
  public void setTimestamp( int parameterIndex, Timestamp x ) throws SQLException
    {
    parent.setTimestamp( parameterIndex, x );
    }

  @Override
  public void setAsciiStream( int parameterIndex, InputStream x, int length ) throws SQLException
    {
    parent.setAsciiStream( parameterIndex, x, length );
    }

  @Override
  @Deprecated
  public void setUnicodeStream( int parameterIndex, InputStream x, int length ) throws SQLException
    {
    parent.setUnicodeStream( parameterIndex, x, length );
    }

  @Override
  public void setBinaryStream( int parameterIndex, InputStream x, int length ) throws SQLException
    {
    parent.setBinaryStream( parameterIndex, x, length );
    }

  @Override
  public void clearParameters() throws SQLException
    {
    parent.clearParameters();
    }

  @Override
  public void setObject( int parameterIndex, Object x, int targetSqlType ) throws SQLException
    {
    parent.setObject( parameterIndex, x, targetSqlType );
    }

  @Override
  public void setObject( int parameterIndex, Object x ) throws SQLException
    {
    parent.setObject( parameterIndex, x );
    }

  @Override
  public boolean execute() throws SQLException
    {
    return parent.execute();
    }

  @Override
  public void addBatch() throws SQLException
    {
    parent.addBatch();
    }

  @Override
  public void setCharacterStream( int parameterIndex, Reader reader, int length ) throws SQLException
    {
    parent.setCharacterStream( parameterIndex, reader, length );
    }

  @Override
  public void setRef( int parameterIndex, Ref x ) throws SQLException
    {
    parent.setRef( parameterIndex, x );
    }

  @Override
  public void setBlob( int parameterIndex, Blob x ) throws SQLException
    {
    parent.setBlob( parameterIndex, x );
    }

  @Override
  public void setClob( int parameterIndex, Clob x ) throws SQLException
    {
    parent.setClob( parameterIndex, x );
    }

  @Override
  public void setArray( int parameterIndex, Array x ) throws SQLException
    {
    parent.setArray( parameterIndex, x );
    }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException
    {
    return parent.getMetaData();
    }

  @Override
  public void setDate( int parameterIndex, Date x, Calendar cal ) throws SQLException
    {
    parent.setDate( parameterIndex, x, cal );
    }

  @Override
  public void setTime( int parameterIndex, Time x, Calendar cal ) throws SQLException
    {
    parent.setTime( parameterIndex, x, cal );
    }

  @Override
  public void setTimestamp( int parameterIndex, Timestamp x, Calendar cal ) throws SQLException
    {
    parent.setTimestamp( parameterIndex, x, cal );
    }

  @Override
  public void setNull( int parameterIndex, int sqlType, String typeName ) throws SQLException
    {
    parent.setNull( parameterIndex, sqlType, typeName );
    }

  @Override
  public void setURL( int parameterIndex, URL x ) throws SQLException
    {
    parent.setURL( parameterIndex, x );
    }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException
    {
    return parent.getParameterMetaData();
    }

  @Override
  public void setRowId( int parameterIndex, RowId x ) throws SQLException
    {
    parent.setRowId( parameterIndex, x );
    }

  @Override
  public void setNString( int parameterIndex, String value ) throws SQLException
    {
    parent.setNString( parameterIndex, value );
    }

  @Override
  public void setNCharacterStream( int parameterIndex, Reader value, long length ) throws SQLException
    {
    parent.setNCharacterStream( parameterIndex, value, length );
    }

  @Override
  public void setNClob( int parameterIndex, NClob value ) throws SQLException
    {
    parent.setNClob( parameterIndex, value );
    }

  @Override
  public void setClob( int parameterIndex, Reader reader, long length ) throws SQLException
    {
    parent.setClob( parameterIndex, reader, length );
    }

  @Override
  public void setBlob( int parameterIndex, InputStream inputStream, long length ) throws SQLException
    {
    parent.setBlob( parameterIndex, inputStream, length );
    }

  @Override
  public void setNClob( int parameterIndex, Reader reader, long length ) throws SQLException
    {
    parent.setNClob( parameterIndex, reader, length );
    }

  @Override
  public void setSQLXML( int parameterIndex, SQLXML xmlObject ) throws SQLException
    {
    parent.setSQLXML( parameterIndex, xmlObject );
    }

  @Override
  public void setObject( int parameterIndex, Object x, int targetSqlType, int scaleOrLength ) throws SQLException
    {
    parent.setObject( parameterIndex, x, targetSqlType, scaleOrLength );
    }

  @Override
  public void setAsciiStream( int parameterIndex, InputStream x, long length ) throws SQLException
    {
    parent.setAsciiStream( parameterIndex, x, length );
    }

  @Override
  public void setBinaryStream( int parameterIndex, InputStream x, long length ) throws SQLException
    {
    parent.setBinaryStream( parameterIndex, x, length );
    }

  @Override
  public void setCharacterStream( int parameterIndex, Reader reader, long length ) throws SQLException
    {
    parent.setCharacterStream( parameterIndex, reader, length );
    }

  @Override
  public void setAsciiStream( int parameterIndex, InputStream x ) throws SQLException
    {
    parent.setAsciiStream( parameterIndex, x );
    }

  @Override
  public void setBinaryStream( int parameterIndex, InputStream x ) throws SQLException
    {
    parent.setBinaryStream( parameterIndex, x );
    }

  @Override
  public void setCharacterStream( int parameterIndex, Reader reader ) throws SQLException
    {
    parent.setCharacterStream( parameterIndex, reader );
    }

  @Override
  public void setNCharacterStream( int parameterIndex, Reader value ) throws SQLException
    {
    parent.setNCharacterStream( parameterIndex, value );
    }

  @Override
  public void setClob( int parameterIndex, Reader reader ) throws SQLException
    {
    parent.setClob( parameterIndex, reader );
    }

  @Override
  public void setBlob( int parameterIndex, InputStream inputStream ) throws SQLException
    {
    parent.setBlob( parameterIndex, inputStream );
    }

  @Override
  public void setNClob( int parameterIndex, Reader reader ) throws SQLException
    {
    parent.setNClob( parameterIndex, reader );
    }
  }
