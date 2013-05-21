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

package cascading.lingual.platform;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.planner.PlatformInfo;
import cascading.lingual.catalog.CatalogManager;
import cascading.lingual.catalog.FileCatalogManager;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.jdbc.Driver;
import cascading.lingual.jdbc.LingualConnection;
import cascading.lingual.optiq.meta.Branch;
import cascading.lingual.util.Reflection;
import cascading.management.CascadingServices;
import cascading.operation.DebugLevel;
import cascading.provider.ServiceLoader;
import cascading.tap.Tap;
import cascading.tap.type.FileType;
import cascading.tuple.TupleEntryCollector;
import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.lingual.jdbc.Driver.*;
import static cascading.lingual.util.Misc.createUniqueName;

/**
 * Class PlatformBroker is the base class of all platform specific implementations.
 * <p/>
 * An instance of a PlatformBroker is created for the requested platform, where all implementations
 * encapsulate necessary specific services.
 * <p/>
 * PlatformBroker instances are returned by the {@link PlatformBrokerFactory}.
 */
public abstract class PlatformBroker<Config>
  {
  private static final Logger LOG = LoggerFactory.getLogger( PlatformBroker.class );

  public static final String META_DATA_PATH_PROP = "lingual.meta-data.path";
  public static final String META_DATA_PATH = ".lingual";

  public static final String CATALOG_FILE_PROP = "lingual.catalog.name";
  public static final String CATALOG_FILE = "catalog";

  public static final String PLANNER_DEBUG = "lingual.planner.debug";

  private Properties properties;

  private CascadingServices cascadingServices;
  private CatalogManager catalogManager;

  private SchemaCatalog catalog;

  private Map<String, TupleEntryCollector> collectorCache;

  protected PlatformBroker()
    {
    }

  public void setProperties( Properties properties )
    {
    this.properties = properties;
    }

  public Properties getProperties()
    {
    if( properties == null )
      properties = new Properties();

    return properties;
    }

  public abstract String getName();

  public abstract Config getConfig();

  public CascadingServices getCascadingServices()
    {
    if( cascadingServices == null )
      cascadingServices = new CascadingServices( getProperties() );

    return cascadingServices;
    }

  public void startConnection( LingualConnection connection ) throws SQLException
    {
    getCatalog().addSchemasTo( connection );
    }

  public synchronized void closeConnection( LingualConnection connection )
    {
    closeCollectorCache();
    }

  public synchronized void enableCollectorCache()
    {
    LOG.info( "enabling collector cache" );
    collectorCache = Collections.synchronizedMap( new HashMap<String, TupleEntryCollector>() );
    }

  public synchronized void disableCollectorCache()
    {
    if( collectorCache == null )
      return;

    if( !collectorCache.isEmpty() )
      throw new IllegalStateException( "must close collector cache before disabling" );

    collectorCache = null;
    }

  public void closeCollectorCache()
    {
    if( collectorCache == null )
      return;

    for( String identifier : collectorCache.keySet() )
      {
      try
        {
        LOG.debug( "closing: {}", identifier );
        collectorCache.get( identifier ).close();
        }
      catch( Exception exception )
        {
        LOG.error( "failed closing collector for: {}", identifier, exception );
        }
      }

    collectorCache.clear();
    }

  public Map<String, TupleEntryCollector> getCollectorCache()
    {
    return collectorCache;
    }

  public abstract FlowProcess<Config> getFlowProcess();

  public DebugLevel getDebugLevel()
    {
    String plannerVerbose = getProperties().getProperty( PLANNER_DEBUG, DebugLevel.NONE.toString() );

    return DebugLevel.valueOf( plannerVerbose.toUpperCase() );
    }

  public boolean catalogLoaded()
    {
    return catalog != null;
    }

  public synchronized SchemaCatalog getCatalog()
    {
    if( catalog == null )
      catalog = loadCatalog();

    return catalog;
    }

  public boolean initializeMetaData()
    {
    String path = getFullMetadataPath();

    if( pathExists( path ) )
      return true;

    if( !createPath( path ) )
      throw new RuntimeException( "unable to create catalog: " + path );

    return false;
    }

  public String getFullMetadataPath()
    {
    String catalogPath = getStringProperty( CATALOG_PROP );

    return makeFullMetadataFilePath( catalogPath );
    }

  public String getFullCatalogPath()
    {
    String catalogPath = getStringProperty( CATALOG_PROP );

    return makeFullCatalogFilePath( catalogPath );
    }

  public void writeCatalog()
    {
    getCatalogManager().writeCatalog( getCatalog() );
    }

  private synchronized SchemaCatalog loadCatalog()
    {
    catalog = getCatalogManager().readCatalog();

    if( catalog == null )
      catalog = newCatalogInstance();

    // schema and tables beyond here are not persisted in the catalog
    // they are transient to the session
    // todo: wrap transient catalog data around persistent catalog data
    if( properties.containsKey( SCHEMAS_PROP ) )
      loadSchemas( catalog );

    if( properties.containsKey( TABLES_PROP ) )
      loadTables( catalog );

    return catalog;
    }

  protected synchronized CatalogManager getCatalogManager()
    {
    if( catalogManager != null )
      return catalogManager;

    catalogManager = loadCatalogManagerPlugin();

    if( catalogManager == null )
      catalogManager = new FileCatalogManager( this );

    return catalogManager;
    }

  private CatalogManager loadCatalogManagerPlugin()
    {
    // getServiceUtil is a private method, this allows for an impl to be loaded from an internal classloader
    ServiceLoader loader = Reflection.invokeInstanceMethod( getCascadingServices(), "getServiceUtil" );
    Properties defaultProperties = Reflection.getStaticField( getCascadingServices().getClass(), "defaultProperties" );

    return (CatalogManager) loader.loadServiceFrom( defaultProperties, getProperties(), CatalogManager.CATALOG_SERVICE_CLASS_PROPERTY );
    }

  private String makeFullMetadataFilePath( String catalogPath )
    {
    String metaDataPath = properties.getProperty( META_DATA_PATH_PROP, META_DATA_PATH );

    return getFullPath( makePath( getFileSeparator(), catalogPath, metaDataPath ) );
    }

  private String makeFullCatalogFilePath( String catalogPath )
    {
    String metaDataPath = properties.getProperty( META_DATA_PATH_PROP, META_DATA_PATH );
    String metaDataFile = properties.getProperty( CATALOG_FILE_PROP, CATALOG_FILE );

    return getFullPath( makePath( getFileSeparator(), catalogPath, metaDataPath, metaDataFile ) );
    }

  public String getResultPath( String name )
    {
    String path = getTempPath();

    path = getProperties().getProperty( Driver.RESULT_PATH_PROP, path );

    if( !path.endsWith( "/" ) )
      path += "/";

    return getFullPath( path + name );
    }

  public String getTempPath( String name )
    {
    String path = getTempPath();

    if( !path.endsWith( "/" ) )
      path += "/";

    return getFullPath( path + name );
    }

  public static String makePath( String fileSeparator, String rootPath, String... elements )
    {
    if( rootPath == null )
      rootPath = ".";

    if( !rootPath.endsWith( fileSeparator ) )
      rootPath += fileSeparator;

    return rootPath + Util.join( elements, fileSeparator );
    }

  public abstract String getFileSeparator();

  public abstract String getTempPath();

  public abstract String getFullPath( String identifier );

  public abstract boolean pathExists( String path );

  public abstract boolean deletePath( String path );

  public abstract boolean createPath( String path );

  public abstract InputStream getInputStream( String path );

  public abstract OutputStream getOutputStream( String path );

  public String createSchemaNameFrom( String identifier )
    {
    String path = URI.create( identifier ).getPath();
    String schemaName = path.replaceAll( "^.*/([^/]+)/?$", "$1" );

    LOG.debug( "found schema name: {}", schemaName );

    return schemaName;
    }

  public String createTableNameFrom( String identifier )
    {
    String path = URI.create( identifier ).getPath();
    String tableName = path.replaceAll( "^.*/([^/.]+)(\\.?.*$|/$)", "$1" );

    LOG.debug( "found table name: {}", tableName );

    return tableName;
    }

  private void loadSchemas( SchemaCatalog catalog )
    {
    String schemaProperty = getStringProperty( SCHEMAS_PROP );
    String[] schemaIdentifiers = schemaProperty.split( "," );

    for( String schemaIdentifier : schemaIdentifiers )
      catalog.createSchemaDefAndTableDefsFor( schemaIdentifier );
    }

  private void loadTables( SchemaCatalog catalog )
    {
    String tableProperty = getStringProperty( TABLES_PROP );
    String[] tableIdentifiers = tableProperty.split( "," );

    for( String tableIdentifier : tableIdentifiers )
      catalog.createTableDefFor( tableIdentifier );
    }

  private String getStringProperty( String propertyName )
    {
    return properties.getProperty( propertyName );
    }

  public abstract Class<? extends SchemaCatalog> getCatalogClass();

  public String[] getChildIdentifiers( String identifier ) throws IOException
    {
    return getChildIdentifiers( getFileTypeFor( identifier ) );
    }

  public abstract FileType getFileTypeFor( String identifier );

  public String[] getChildIdentifiers( FileType<Config> fileType ) throws IOException
    {
    if( !( (Tap) fileType ).resourceExists( getConfig() ) )
      throw new IllegalStateException( "resource does not exist: " + ( (Tap) fileType ).getFullIdentifier( getConfig() ) );

    return fileType.getChildIdentifiers( getConfig() );
    }

  public PlatformInfo getPlatformInfo()
    {
    return getFlowConnector().getPlatformInfo();
    }

  public abstract FlowConnector getFlowConnector();

  public LingualFlowFactory getFlowFactory( Branch branch )
    {
    return new LingualFlowFactory( this, createUniqueName(), branch );
    }

  public SchemaCatalog newCatalogInstance()
    {
    try
      {
      SchemaCatalog schemaCatalog = getCatalogClass().getConstructor().newInstance();

      schemaCatalog.setPlatformBroker( this );

      schemaCatalog.initializeNew(); // initialize defaults for a new catalog and root schema

      return schemaCatalog;
      }
    catch( Exception exception )
      {
      throw new RuntimeException( "unable to construct class: " + getCatalogClass().getName(), exception );
      }
    }

  protected String findActualPath( String parentIdentifier, String identifier )
    {
    try
      {
      String[] childIdentifiers = getFileTypeFor( parentIdentifier ).getChildIdentifiers( getConfig() );

      for( String child : childIdentifiers )
        {
        if( child.equalsIgnoreCase( identifier ) )
          return child;
        }
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to get full path: " + identifier, exception );
      }

    return identifier;
    }
  }
