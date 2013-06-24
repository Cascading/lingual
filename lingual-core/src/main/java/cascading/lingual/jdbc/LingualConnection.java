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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.flow.Flow;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.optiq.FieldTypeFactory;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.platform.PlatformBrokerFactory;
import cascading.tuple.Fields;
import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.lingual.jdbc.Driver.*;

/**
 * Class LingualConnection is the base class for all Driver connection instances.
 * <p/>
 * This class is dynamically sub-classed to support the available JDBC version in the current JVM.
 */
public abstract class LingualConnection implements Connection
  {
  private static final Logger LOG = LoggerFactory.getLogger( LingualConnection.class );

  private static FieldTypeFactory typeFactory = new FieldTypeFactory();

  private OptiqConnection parent;
  private Properties properties;
  private PlatformBroker platformBroker;

  // see JavaDoc on LingualConnectionFlowListener for why this is a Collection.
  private Set<Flow> trackedFlows = new HashSet<Flow>();

  protected LingualConnection( Connection parent, Properties properties ) throws SQLException
    {
    this.parent = parent.unwrap( OptiqConnection.class );
    this.properties = properties;

    // log errors fully here in case the calling platform does a poor job of error reporting.
    try
      {
      initialize();
      }
    catch( SQLException sqlException )
      {
      String providerError = String.format( "connection failed: %s ( provider %s error code %d).", sqlException.getMessage(), parent.getMetaData().getDatabaseProductName(), sqlException.getErrorCode() );
      LOG.error( providerError );
      LOG.error( "\tconnection URL: " + parent.getMetaData().getURL() );
      if( platformBroker != null )
        {
        LOG.error( "\tread catalog from: " + platformBroker.getFullCatalogPath() );
        if( platformBroker.getCatalog() != null && platformBroker.getCatalog().getRootSchemaDef() != null )
          LOG.error( "\tused root schema from: " + platformBroker.getCatalog().getRootSchemaDef().getIdentifier() );
        else
          LOG.error( "\teither catalog or root schema not set." );
        }
      else
        {
        LOG.error( "\tunable to create platform " + getStringProperty( PLATFORM_PROP ) + ": {}", sqlException.getMessage() );
        }
      throw sqlException;
      }
    }

  private void initialize() throws SQLException
    {
    String platformName = getStringProperty( PLATFORM_PROP );

    if( platformName == null )
      platformName = "local";

    LOG.info( "using platform: {}", platformName );

    String schemaName = getStringProperty( SCHEMA_PROP );

    if( schemaName != null )
      {
      setSchema( schemaName );
      LOG.info( "using schema: {}", schemaName );
      }
    else
      {
      LOG.info( "using default schema." );
      }

    platformBroker = PlatformBrokerFactory.createPlatformBroker( platformName, properties );

    if( platformBroker == null )
      throw new SQLException( "no platform broker for " + platformName );

    setAutoCommit( !isCollectorCacheEnabled() ); // this forces the default to true

    platformBroker.startConnection( this );
    }

  protected boolean isCollectorCacheEnabled()
    {
    String collectorCache = getStringProperty( COLLECTOR_CACHE_PROP );

    return collectorCache != null && Boolean.parseBoolean( collectorCache );
    }

  public PlatformBroker getPlatformBroker()
    {
    return platformBroker;
    }

  public OptiqConnection getParent()
    {
    return parent;
    }

  public FieldTypeFactory getTypeFactory()
    {
    return typeFactory;
    }

  public MutableSchema getRootSchema()
    {
    return parent.getRootSchema();
    }

  private String getStringProperty( String propertyName )
    {
    return properties.getProperty( propertyName );
    }

  public void addTable( String schemaName, String tableName, String identifier, Fields fields, String protocolName, String formatName ) throws SQLException
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    if( catalog.getSchemaDef( schemaName ) == null )
      catalog.addSchemaDef( schemaName, protocolName, formatName );

    catalog.createTableDefFor( schemaName, tableName, identifier, fields, protocolName, formatName );
    catalog.addSchemasTo( this );
    }

  public void trackFlow( Flow flow )
    {
    trackedFlows.add( flow );
    }

  public void unTrackFlow( Flow flow )
    {
    trackedFlows.remove( flow );
    }

  public Flow getCurrentFlow()
    {
    // see JavaDoc on LingualConnectionFlowListener for why this behavior exists
    if( trackedFlows.size() == 1 )
      return trackedFlows.iterator().next();

    LOG.error( "unable to determine single current flow. found {} flows.", trackedFlows.size() );

    return null;
    }

  // Connection methods
  public void setSchema( String schema ) throws SQLException
    {
    parent.setSchema( schema );
    }

  @Override
  public Statement createStatement() throws SQLException
    {
    return new LingualStatement( properties, parent.createStatement(), this );
    }

  @Override
  public PreparedStatement prepareStatement( String sql ) throws SQLException
    {
    return new LingualPreparedStatement( properties, parent.prepareStatement( sql ), this );
    }

  @Override
  public CallableStatement prepareCall( String sql ) throws SQLException
    {
    return parent.prepareCall( sql );
    }

  @Override
  public String nativeSQL( String sql ) throws SQLException
    {
    return parent.nativeSQL( sql );
    }

  @Override
  public void setAutoCommit( boolean autoCommit ) throws SQLException
    {
    parent.setAutoCommit( autoCommit );

    if( autoCommit )
      platformBroker.disableCollectorCache();
    else
      platformBroker.enableCollectorCache();
    }

  @Override
  public boolean getAutoCommit() throws SQLException
    {
    return parent.getAutoCommit();
    }

  @Override
  public void commit() throws SQLException
    {
    // parent.commit(); // not supported

    platformBroker.closeCollectorCache();
    }

  @Override
  public void rollback() throws SQLException
    {
    // parent.rollback(); // not supported

    // todo: close and delete pending cached items
    }

  @Override
  public void close() throws SQLException
    {
    try
      {
      parent.close();
      }
    finally
      {
      platformBroker.closeConnection( this );
      }
    }

  @Override
  public boolean isClosed() throws SQLException
    {
    return parent.isClosed();
    }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException
    {
    return new LingualDatabaseMetaData( this, parent.getMetaData() );
    }

  @Override
  public void setReadOnly( boolean readOnly ) throws SQLException
    {
    parent.setReadOnly( readOnly );
    }

  @Override
  public boolean isReadOnly() throws SQLException
    {
    return parent.isReadOnly();
    }

  @Override
  public void setCatalog( String catalog ) throws SQLException
    {
    parent.setCatalog( catalog );
    }

  @Override
  public String getCatalog() throws SQLException
    {
    return parent.getCatalog();
    }

  @Override
  public void setTransactionIsolation( int level ) throws SQLException
    {
    parent.setTransactionIsolation( level );
    }

  @Override
  public int getTransactionIsolation() throws SQLException
    {
    return parent.getTransactionIsolation();
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
  public Statement createStatement( int resultSetType, int resultSetConcurrency ) throws SQLException
    {
    return parent.createStatement( resultSetType, resultSetConcurrency );
    }

  @Override
  public PreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency ) throws SQLException
    {
    return parent.prepareStatement( sql, resultSetType, resultSetConcurrency );
    }

  @Override
  public CallableStatement prepareCall( String sql, int resultSetType, int resultSetConcurrency ) throws SQLException
    {
    return parent.prepareCall( sql, resultSetType, resultSetConcurrency );
    }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException
    {
    return parent.getTypeMap();
    }

  @Override
  public void setTypeMap( Map<String, Class<?>> map ) throws SQLException
    {
    parent.setTypeMap( map );
    }

  @Override
  public void setHoldability( int holdability ) throws SQLException
    {
    parent.setHoldability( holdability );
    }

  @Override
  public int getHoldability() throws SQLException
    {
    return parent.getHoldability();
    }

  @Override
  public Savepoint setSavepoint() throws SQLException
    {
    //note that Optiq does not currently support savepoints
    return parent.setSavepoint();
    }

  @Override
  public Savepoint setSavepoint( String name ) throws SQLException
    {
    //note that Optiq does not currently support savepoints
    return parent.setSavepoint( name );
    }

  @Override
  public void rollback( Savepoint savepoint ) throws SQLException
    {
    //note that Optiq does not currently support savepoints
    parent.rollback( savepoint );
    }

  @Override
  public void releaseSavepoint( Savepoint savepoint ) throws SQLException
    {
    //note that Optiq does not currently support savepoints
    parent.releaseSavepoint( savepoint );
    }

  @Override
  public Statement createStatement( int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException
    {
    return parent.createStatement( resultSetType, resultSetConcurrency, resultSetHoldability );
    }

  @Override
  public PreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException
    {
    return parent.prepareStatement( sql, resultSetType, resultSetConcurrency, resultSetHoldability );
    }

  @Override
  public CallableStatement prepareCall( String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException
    {
    return parent.prepareCall( sql, resultSetType, resultSetConcurrency, resultSetHoldability );
    }

  @Override
  public PreparedStatement prepareStatement( String sql, int autoGeneratedKeys ) throws SQLException
    {
    return parent.prepareStatement( sql, autoGeneratedKeys );
    }

  @Override
  public PreparedStatement prepareStatement( String sql, int[] columnIndexes ) throws SQLException
    {
    return parent.prepareStatement( sql, columnIndexes );
    }

  @Override
  public PreparedStatement prepareStatement( String sql, String[] columnNames ) throws SQLException
    {
    return parent.prepareStatement( sql, columnNames );
    }

  @Override
  public Clob createClob() throws SQLException
    {
    return parent.createClob();
    }

  @Override
  public Blob createBlob() throws SQLException
    {
    return parent.createBlob();
    }

  @Override
  public NClob createNClob() throws SQLException
    {
    return parent.createNClob();
    }

  @Override
  public SQLXML createSQLXML() throws SQLException
    {
    return parent.createSQLXML();
    }

  @Override
  public boolean isValid( int timeout ) throws SQLException
    {
    return parent.isValid( timeout );
    }

  @Override
  public void setClientInfo( String name, String value ) throws SQLClientInfoException
    {
    parent.setClientInfo( name, value );
    }

  @Override
  public void setClientInfo( Properties properties ) throws SQLClientInfoException
    {
    parent.setClientInfo( properties );
    }

  @Override
  public String getClientInfo( String name ) throws SQLException
    {
    return parent.getClientInfo( name );
    }

  @Override
  public Properties getClientInfo() throws SQLException
    {
    return parent.getClientInfo();
    }

  @Override
  public Array createArrayOf( String typeName, Object[] elements ) throws SQLException
    {
    return parent.createArrayOf( typeName, elements );
    }

  @Override
  public Struct createStruct( String typeName, Object[] attributes ) throws SQLException
    {
    return parent.createStruct( typeName, attributes );
    }

  public <T> T unwrap( Class<T> iface ) throws SQLException
    {
    if( iface.isInstance( this ) )
      return iface.cast( this );

    else if( iface.isInstance( parent ) )
      return iface.cast( parent );

    throw new SQLException( "does not implement '" + iface + "'" );
    }

  public boolean isWrapperFor( Class<?> iface ) throws SQLException
    {
    return iface.isInstance( this ) || iface.isInstance( parent );
    }
  }
