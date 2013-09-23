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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.ref.WeakReference;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import cascading.bind.catalog.Stereotype;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.planner.PlatformInfo;
import cascading.lingual.catalog.CatalogManager;
import cascading.lingual.catalog.Def;
import cascading.lingual.catalog.FileCatalogManager;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.SchemaDef;
import cascading.lingual.catalog.TableDef;
import cascading.lingual.jdbc.Driver;
import cascading.lingual.jdbc.LingualConnection;
import cascading.lingual.optiq.meta.Branch;
import cascading.lingual.optiq.meta.Ref;
import cascading.lingual.util.Reflection;
import cascading.management.CascadingServices;
import cascading.operation.DebugLevel;
import cascading.provider.ServiceLoader;
import cascading.tap.Tap;
import cascading.tap.type.FileType;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.util.Util;
import com.google.common.io.ByteStreams;
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

  public static final String META_DATA_DIR_NAME_PROP = "lingual.meta-data.path";
  public static final String META_DATA_DIR_NAME = ".lingual"; // under path pointed to by Driver.CATALOG_ROOT_PATH_PROP

  public static final String CATALOG_FILE_NAME_PROP = "lingual.catalog.name";
  public static final String CATALOG_FILE_NAME = "catalog"; // json, under META_DATA_DIR_NAME

  public static final String PROVIDER_DIR_NAME_PROP = "lingual.providers.dir";
  public static final String PROVIDER_DIR_NAME = "providers"; // dir for provider (fat) jars installed with --provider --add

  public static final String PLANNER_DEBUG_PROP = "lingual.planner.debug";

  public static final String CONFIG_DIR_NAME_PROP = "lingual.config.dir";
  public static final String CONFIG_DIR_NAME = "config"; // dir under META_DATA_DIR_NAME

  public static final String CONFIG_FILE_NAME_PROP = "lingual.config.name";
  public static final String CONFIG_FILE_NAME = "default.properties";

  protected Properties properties;

  private CascadingServices cascadingServices;
  private CatalogManager catalogManager;

  private SchemaCatalog catalog;

  private Map<String, TupleEntryCollector> collectorCache;

  private WeakReference<LingualConnection> nextConnection; // Only used to bind a CTRL-C listener in single-connection CLI usage.

  private String resultsSchemaName;

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

  private String getStringProperty( String propertyName )
    {
    return properties.getProperty( propertyName );
    }

  protected String getStringProperty( String propertyName, String defaultString )
    {
    return properties.getProperty( propertyName, defaultString );
    }

  public abstract String getName();

  public abstract Config getDefaultConfig();

  public abstract Config getSystemConfig();

  public abstract Config getPlannerConfig();

  public CascadingServices getCascadingServices()
    {
    if( cascadingServices == null )
      cascadingServices = new CascadingServices( getProperties() );

    return cascadingServices;
    }

  public void startConnection( LingualConnection connection ) throws SQLException
    {
    LOG.debug( "starting connection" );

    try
      {
      getCatalog().addSchemasTo( connection );
      nextConnection = new WeakReference<LingualConnection>( connection );
      }
    catch( Throwable throwable )
      {
      LOG.error( "error starting connection", throwable );
      throw new SQLException( throwable );
      }

    LOG.debug( "connection started" );
    }

  public synchronized void closeConnection( LingualConnection connection )
    {
    LOG.info( "closing connection" );

    try
      {
      closeCollectorCache();
      nextConnection.clear();
      }
    catch( Throwable throwable )
      {
      LOG.error( "unable to close connection", throwable );
      throw new RuntimeException( "unable to close connection", throwable );
      }
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
    String plannerVerbose = getProperties().getProperty( Driver.PLANNER_DEBUG, DebugLevel.NONE.toString() );

    return DebugLevel.valueOf( plannerVerbose.toUpperCase() );
    }

  public boolean catalogLoaded()
    {
    return catalog != null;
    }

  public synchronized SchemaCatalog getCatalog()
    {
    // this will only be null on startup so exit the sync block asap.
    if( catalog != null )
      return catalog;

    catalog = loadCatalog();

    return catalog;
    }

  public boolean confirmMetaData()
    {
    String path = getFullMetadataPath();

    return pathExists( path );
    }

  public boolean initializeMetaData()
    {
    String path = getFullMetadataPath();

    if( pathExists( path ) )
      return false;

    if( !createPath( path ) )
      throw new RuntimeException( "unable to create catalog: " + path );

    writeDefaultConfigFile();

    return true;
    }

  private void writeDefaultConfigFile()
    {
    if( !createPath( getFullConfigPath() ) )
      throw new RuntimeException( "unable to create config path: " + getFullConfigPath() );

    if( !writeToFile( getFullConfigFile(), "# place all default properties here, for example\n# some.property=someValue\n" ) )
      throw new RuntimeException( "unable to create config file: " + getFullConfigFile() );
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

  public String getFullProviderPath()
    {
    String catalogPath = getStringProperty( CATALOG_PROP );

    return makeFullProviderDirPath( catalogPath );
    }

  public String getFullConfigPath()
    {
    String catalogPath = getStringProperty( CATALOG_PROP );

    return makeFullConfigPath( catalogPath );
    }

  public String getFullConfigFile()
    {
    return makePath( getFullConfigPath(), getStringProperty( CONFIG_FILE_NAME_PROP, CONFIG_FILE_NAME ) );
    }

  public void writeCatalog()
    {
    getCatalogManager().writeCatalog( getCatalog() );
    }

  protected SchemaCatalog loadCatalog()
    {
    SchemaCatalog catalog = getCatalogManager().readCatalog();

    if( catalog == null )
      catalog = newCatalogInstance();

    // schema and tables beyond here are not persisted in the catalog
    // they are transient to the session
    // todo: wrap transient catalog data around persistent catalog data
    if( getProperties().containsKey( SCHEMAS_PROP ) )
      loadTransientSchemas( catalog );

    if( getProperties().containsKey( TABLES_PROP ) )
      loadTransientTables( catalog );

    if( getProperties().containsKey( RESULT_SCHEMA_PROP ) )
      loadTransientResultSchema( catalog );

    return catalog;
    }

  public void addResultToSchema( Tap tap, LingualConnection lingualConnection )
    {
    getCatalog().addTapToConnection( lingualConnection, resultsSchemaName, tap, "LAST" );
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
    String metaDataPath = properties.getProperty( META_DATA_DIR_NAME_PROP, META_DATA_DIR_NAME );

    return getFullPath( makePath( catalogPath, metaDataPath ) );
    }

  private String makeFullCatalogFilePath( String catalogPath )
    {
    String catalogFile = properties.getProperty( CATALOG_FILE_NAME_PROP, CATALOG_FILE_NAME );

    return getFullPath( makePath( makeFullMetadataFilePath( catalogPath ), catalogFile ) );
    }

  private String makeFullProviderDirPath( String catalogPath )
    {
    String providerDir = properties.getProperty( PROVIDER_DIR_NAME_PROP, PROVIDER_DIR_NAME );

    return getFullPath( makePath( makeFullMetadataFilePath( catalogPath ), providerDir ) );
    }

  private String makeFullConfigPath( String catalogPath )
    {
    String configDir = properties.getProperty( CONFIG_DIR_NAME_PROP, CONFIG_DIR_NAME );

    return getFullPath( makePath( makeFullMetadataFilePath( catalogPath ), configDir ) );
    }

  public boolean hasResultSchemaDef()
    {
    return resultsSchemaName != null;
    }

  public SchemaDef getResultSchemaDef()
    {
    return getCatalog().getSchemaDef( resultsSchemaName );
    }

  public String getResultPath( String name )
    {
    return makePath( getRootResultPath(), name );
    }

  protected String getRootResultPath()
    {
    if( resultsSchemaName != null )
      return getCatalog().getSchemaDef( resultsSchemaName ).getIdentifier();
    else
      return getProperties().getProperty( Driver.RESULT_PATH_PROP, getTempPath() );
    }

  public String getTempPath( String name )
    {
    String path = getTempPath();

    if( !path.endsWith( "/" ) )
      path += "/";

    return getFullPath( path + name );
    }

  public String makePath( String rootPath, String... elements )
    {
    return buildPath( getFileSeparator(), rootPath, elements );
    }

  public static String buildPath( String fileSeparator, String rootPath, String... elements )
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

  private boolean writeToFile( String fileName, String string )
    {
    try
      {
      Writer writer = new OutputStreamWriter( getOutputStream( fileName ) );

      writer.write( string );
      writer.flush();
      writer.close();
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to write to file: {}", fileName );
      return false;
      }

    return true;
    }

  public String retrieveInstallProvider( String sourcePath )
    {
    File sourceFile = new File( sourcePath );
    String targetPath = makePath( getFullProviderPath(), sourceFile.getName() );

    if( pathExists( targetPath ) )
      {
      LOG.info( "replacing target {}", targetPath );
      deletePath( targetPath );
      }

    long bytesCopied;
    InputStream inputStream = null;
    OutputStream outputStream = null;

    try
      {
      inputStream = new FileInputStream( sourceFile );
      outputStream = getOutputStream( targetPath );
      bytesCopied = ByteStreams.copy( inputStream, outputStream );
      outputStream.flush();
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to copy from " + sourcePath + " to " + targetPath + ":" + exception.getMessage(), exception );
      }
    finally
      {
      try
        {
        if( inputStream != null )
          inputStream.close();

        if( outputStream != null )
          outputStream.close();
        }
      catch( IOException exception )
        {
        LOG.error( "error closing file {}: ", targetPath, exception );
        }
      }

    if( LOG.isDebugEnabled() )
      LOG.debug( "copied bytes: {} to: {}", bytesCopied, targetPath );

    if( bytesCopied > 0 )
      return new File( targetPath ).getName(); // return relative path to provider install directory
    else
      throw new RuntimeException( "zero bytes copied from " + sourcePath + " to " + targetPath );
    }

  public URI retrieveTempProvider( String providerJar )
    {
    long bytesCopied;
    InputStream inputStream = null;
    OutputStream outputStream = null;

    File tempFile = null;
    try
      {
      tempFile = File.createTempFile( "provider", ".jar" );
      inputStream = getInputStream( providerJar );
      outputStream = new FileOutputStream( tempFile );
      bytesCopied = ByteStreams.copy( inputStream, outputStream );
      outputStream.flush();
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to copy from " + providerJar + " to " + tempFile + ":" + exception.getMessage(), exception );
      }
    finally
      {
      try
        {
        if( inputStream != null )
          inputStream.close();

        if( outputStream != null )
          outputStream.close();
        }
      catch( IOException exception )
        {
        LOG.error( "error closing file {}: ", providerJar, exception );
        }
      }

    if( LOG.isDebugEnabled() )
      LOG.debug( "copied bytes: {} to: {}", bytesCopied, providerJar );

    if( bytesCopied > 0 )
      return tempFile.toURI();
    else
      throw new RuntimeException( "zero bytes copied from " + providerJar + " to " + tempFile );
    }

  public String createSchemaNameFrom( String identifier )
    {
    String path = URI.create( identifier ).getPath();
    String schemaName = path.replaceAll( "^.*/([^/]+)/?$", "$1" );

    LOG.debug( "found schema name: {} at: {}", schemaName, path );

    return schemaName;
    }

  public String createTableNameFrom( String identifier )
    {
    String path = URI.create( identifier ).getPath();
    String tableName = path.replaceAll( "^.*/([^/.]+)(\\.?.*$|/$)", "$1" );

    LOG.debug( "found table name: {} at: {}", tableName, path );

    return tableName;
    }

  private void loadTransientResultSchema( SchemaCatalog catalog )
    {
    String schemaProperty = getStringProperty( RESULT_SCHEMA_PROP );

    if( schemaProperty == null )
      return;

    String[] schemaNames = schemaProperty.split( "," );

    if( schemaNames.length > 1 )
      throw new IllegalStateException( "may only have one result schema" );

    String resultPath = getRootResultPath();

    resultsSchemaName = catalog.createResultsSchemaDef( schemaNames[ 0 ], resultPath );
    }

  private void loadTransientSchemas( SchemaCatalog catalog )
    {
    String schemaProperty = getStringProperty( SCHEMAS_PROP );
    String[] schemaIdentifiers = schemaProperty.split( "," );

    for( String schemaIdentifier : schemaIdentifiers )
      catalog.createSchemaDefAndTableDefsFor( schemaIdentifier );
    }

  private void loadTransientTables( SchemaCatalog catalog )
    {
    String tableProperty = getStringProperty( TABLES_PROP );
    String[] tableIdentifiers = tableProperty.split( "," );

    for( String tableIdentifier : tableIdentifiers )
      catalog.createTableDefFor( tableIdentifier );
    }

  public abstract Class<? extends SchemaCatalog> getCatalogClass();

  public String[] getChildIdentifiers( String identifier ) throws IOException
    {
    return getChildIdentifiers( getFileTypeFor( identifier ) );
    }

  public abstract FileType getFileTypeFor( String identifier );

  public String[] getChildIdentifiers( FileType<Config> fileType ) throws IOException
    {
    if( !( (Tap) fileType ).resourceExists( getSystemConfig() ) )
      throw new IllegalStateException( "resource does not exist: " + ( (Tap) fileType ).getFullIdentifier( getSystemConfig() ) );

    return fileType.getChildIdentifiers( getSystemConfig() );
    }

  public PlatformInfo getPlatformInfo()
    {
    return getFlowConnector().getPlatformInfo();
    }

  public abstract FlowConnector getFlowConnector();

  public LingualFlowFactory getFlowFactory( Branch branch )
    {
    LingualFlowFactory lingualFlowFactory = new LingualFlowFactory( this, nextConnection.get(), createUniqueName(), branch.current );

    for( Ref head : branch.heads.keySet() )
      {
      Stereotype<Protocol, Format> stereotypeFor;

      TableDef tableDef = head.tableDef;

      if( tableDef == null )
        stereotypeFor = catalog.getRootSchemaDef().findStereotypeFor( head.fields ); // do not use head name
      else
        stereotypeFor = tableDef.getStereotype();

      lingualFlowFactory.setSourceStereotype( head.name, stereotypeFor );

      if( tableDef != null )
        addHandlers( lingualFlowFactory, tableDef );
      }

    if( branch.tailTableDef != null )
      lingualFlowFactory.setSinkStereotype( branch.current.getName(), branch.tailTableDef.getStereotype() );
    else
      lingualFlowFactory.setSinkStereotype( branch.current.getName(), catalog.getStereoTypeFor( Fields.UNKNOWN ) );

    if( branch.tailTableDef != null )
      addHandlers( lingualFlowFactory, branch.tailTableDef );
    else
      addHandlers( lingualFlowFactory, catalog.getRootSchemaDef().getName(), catalog.getRootSchemaDef() );

    return lingualFlowFactory;
    }

  private void addHandlers( LingualFlowFactory lingualFlowFactory, TableDef tableDef )
    {
    addHandlers( lingualFlowFactory, tableDef.getParentSchema().getName(), tableDef );
    }

  private void addHandlers( LingualFlowFactory lingualFlowFactory, String schemaName, Def def )
    {
    if( !lingualFlowFactory.containsProtocolHandlers( schemaName ) )
      lingualFlowFactory.addProtocolHandlers( schemaName, catalog.getProtocolHandlers( def ) );

    if( !lingualFlowFactory.containsFormatHandlers( schemaName ) )
      lingualFlowFactory.addFormatHandlers( schemaName, catalog.getFormatHandlersFor( def ) );
    }

  public SchemaCatalog newCatalogInstance()
    {
    try
      {
      LOG.info( "creating new SchemaCatalog at {}", getFullCatalogPath() );
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
      String[] childIdentifiers = getFileTypeFor( parentIdentifier ).getChildIdentifiers( getSystemConfig() );

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

  public Class loadClass( String qualifiedPath, String className )
    {
    if( !pathExists( qualifiedPath ) )
      throw new IllegalStateException( "path does not exist: " + qualifiedPath );

    try
      {
      URLClassLoader urlLoader = getUrlClassLoader( qualifiedPath );

      if( LOG.isDebugEnabled() )
        LOG.info( "loading class: " + className );

      return urlLoader.loadClass( className );
      }
    catch( Exception exception )
      {
      throw new RuntimeException( "unable to load class: " + className + " from: " + qualifiedPath, exception );
      }
    }

  Map<Set<String>, URLClassLoader> classLoaderMap = new HashMap<Set<String>, URLClassLoader>();

  public synchronized URLClassLoader getUrlClassLoader( String... qualifiedPaths )
    {
    Set<String> key = new TreeSet<String>( Arrays.asList( qualifiedPaths ) );

    if( classLoaderMap.containsKey( key ) )
      return classLoaderMap.get( key );

    URL[] urls = toURLs( qualifiedPaths );

    if( LOG.isDebugEnabled() )
      LOG.debug( "loading from: {}", Arrays.toString( urls ) );

    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    URLClassLoader urlClassLoader = new URLClassLoader( urls, classLoader, null );

    classLoaderMap.put( key, urlClassLoader );

    return urlClassLoader;
    }

  public URL[] toURLs( String... qualifiedPaths )
    {
    URL[] results = new URL[ qualifiedPaths.length ];

    for( int i = 0; i < qualifiedPaths.length; i++ )
      results[ i ] = toURL( qualifiedPaths[ i ] );

    return results;
    }

  protected URL toURL( String qualifiedPath )
    {
    try
      {
      URI uri = toURI( qualifiedPath );

      if( !uri.getScheme().equals( "file" ) )
        return retrieveTempProvider( qualifiedPath ).toURL();

      URLStreamHandlerFactory handlerFactory = getURLStreamHandlerFactory();

      if( handlerFactory == null )
        return uri.toURL();

      return new URL( null, uri.toString(), handlerFactory.createURLStreamHandler( uri.getScheme() ) );
      }
    catch( MalformedURLException exception )
      {
      throw new IllegalStateException( "unable to parse path: " + qualifiedPath, exception );
      }
    }

  protected abstract URI toURI( String qualifiedPath );

  protected abstract URLStreamHandlerFactory getURLStreamHandlerFactory();

  protected Properties loadPropertiesFrom( URL url )
    {
    Properties properties = new Properties();

    InputStream inputStream = null;

    try
      {
      inputStream = url.openStream();
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to open resource file" );
      }

    return loadPropertiesFrom( properties, inputStream );
    }

  protected Properties loadPropertiesFrom( Properties properties, InputStream inputStream )
    {
    try
      {
      if( inputStream != null )
        properties.load( inputStream );
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to read resource file" );
      }

    return properties;
    }
  }
