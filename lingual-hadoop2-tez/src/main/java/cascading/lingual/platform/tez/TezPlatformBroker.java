/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.lingual.platform.tez;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLStreamHandlerFactory;
import java.sql.SQLException;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.tez.Hadoop2TezFlowConnector;
import cascading.flow.tez.Hadoop2TezFlowProcess;
import cascading.flow.tez.util.TezUtil;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.jdbc.LingualConnection;
import cascading.lingual.platform.PlatformBroker;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.TapException;
import cascading.tap.hadoop.Hfs;
import cascading.tap.type.FileType;
import cascading.tuple.hadoop.BigDecimalSerialization;
import cascading.tuple.hadoop.TupleSerializationProps;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TezPlatformBroker extends PlatformBroker<TezConfiguration>
  {
  private static final Logger LOG = LoggerFactory.getLogger( TezPlatformBroker.class );

  public static final Protocol DEFAULT_PROTOCOL = Protocol.getProtocol( "hdfs" );
  public static final Format DEFAULT_FORMAT = Format.getFormat( "tcsv" );

  //public static final String HADOOP_USER_ENV = "HADOOP_USER_NAME";
  //public static final String HADOOP_USER_PROPERTY = "hadoop.username";
  public static final String TEZ_OVERRIDE_RESOURCE = "tez-override.properties";
  public static final String TEZ_APP_JAR_FLAG_RESOURCE = "hadoop.job.properties";

  private TezConfiguration systemTezConf; // for use by methods accessing the platform
  private TezConfiguration plannerTezConf; // for use by configuring the resulting Flow with job properties

  public TezPlatformBroker()
    {
    LOG.info( "hello world" );
    }

  @Override
  public String getName()
    {
    return "hadoop2-tez";
    }

  @Override
  public Format getDefaultFormat()
    {
    return DEFAULT_FORMAT;
    }

  @Override
  public Protocol getDefaultProtocol()
    {
    return DEFAULT_PROTOCOL;
    }

  @Override
  public void startConnection( LingualConnection connection ) throws SQLException
    {
    Thread thread = Thread.currentThread();
    ClassLoader current = thread.getContextClassLoader();

    // see https://issues.apache.org/jira/browse/HADOOP-7982
    if( classExists( "org.apache.hadoop.security.UserGroupInformation$HadoopLoginModule" ) )
      {
      Class ugi = getClass( "org.apache.hadoop.security.UserGroupInformation$HadoopLoginModule" );

      if( ugi != null )
        thread.setContextClassLoader( ugi.getClassLoader() );
      }

    try
      {
      // first confirm we can talk to hadoop in general.
      try
        {
        FileSystem.getAllStatistics();
        }
      catch( Exception exception )
        {
        throw new SQLException( "unable to connect to Hadoop at " + connection.getMetaData().getURL(), exception );
        }

      super.startConnection( connection );
      }
    finally
      {
      thread.setContextClassLoader( current );
      }
    }

  @Override
  public TezConfiguration getDefaultConfig()
    {
    TezConfiguration tezConfig = new TezConfiguration();
    String fullConfigPath = getFullConfigFile();

    if( !pathExists( fullConfigPath ) )
      return tezConfig;

    LOG.info( "reading default configuration from: {}", fullConfigPath );

    InputStream inputStream = getInputStream( fullConfigPath );

    Properties properties = loadPropertiesFrom( new Properties(), inputStream );

    tezConfig = TezUtil.createTezConf( properties, tezConfig );

    return tezConfig;

    }

  @Override
  public synchronized TezConfiguration getSystemConfig()
    {
    if( systemTezConf != null )
      return systemTezConf;

    // may consider providing aliases for these properties on Driver
    // mapred.job.tracker
    // fs.default.name
    assert properties.getProperty( "lingual.meta-data.path" ) != null;

    Properties configProperties = new Properties( getProperties() );

    // job configuration values
    systemTezConf = TezUtil.createTezConf( configProperties, new TezConfiguration() );

    return systemTezConf;
    }

  @Override
  public synchronized TezConfiguration getPlannerConfig()
    {
    if( plannerTezConf != null )
      return applyClassLoader( plannerTezConf );

    Properties configProperties = new Properties( getProperties() );

    // job configuration values

    TupleSerializationProps.addSerialization( configProperties, BigDecimalSerialization.class.getName() );

    plannerTezConf = TezUtil.createTezConf( configProperties, getDefaultConfig() );

    URL url = getResource( TEZ_OVERRIDE_RESOURCE );

    if( url != null )
      {
      LOG.info( "loading override properties from: {}", url.toString() );

      Properties overrideProperties = loadPropertiesFrom( url );

      for( String propertyName : overrideProperties.stringPropertyNames() )
        plannerTezConf.set( propertyName, overrideProperties.getProperty( propertyName ) );
      }

    return applyClassLoader( plannerTezConf );
    }

  private TezConfiguration applyClassLoader( TezConfiguration tezConfiguration )
    {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if ( classLoader != null && !classLoader.equals( tezConfiguration.getClassLoader() ) )
      tezConfiguration.setClassLoader( classLoader );
    return tezConfiguration;
    }

  private URL getResource( String resourceName )
    {
    return Thread.currentThread().getContextClassLoader().getResource( resourceName );
    }

  private boolean classExists( String classname )
    {
    try
      {
      return Class.forName( classname, false, this.getClass().getClassLoader() ) != null;
      }
    catch( ClassNotFoundException classNotFound )
      {
      return false;
      }
    }

  private Class getClass( String classname )
    {
    try
      {
      return Class.forName( classname );
      }
    catch( ClassNotFoundException classNotFound )
      {
      LOG.error( "could not load class {} that was supposed to be on the classpath", classname );
      return null;
      }
    }

  @Override
  public FlowConnector getFlowConnector()
    {
    return new Hadoop2TezFlowConnector( HadoopUtil.createProperties( getPlannerConfig() ) );
    }

  @Override
  protected URI toURI( String qualifiedPath )
    {
    return new Path( qualifiedPath ).toUri();
    }

  @Override
  protected URLStreamHandlerFactory getURLStreamHandlerFactory()
    {
    return new FsUrlStreamHandlerFactory( getSystemConfig() );
    }

  @Override
  public FlowProcess<TezConfiguration> getFlowProcess()
    {
    return new Hadoop2TezFlowProcess( getPlannerConfig() );
    }

  @Override
  public FileType getFileTypeFor( String identifier )
    {
    return new Hfs( new TextLine(), identifier, SinkMode.KEEP );
    }

  @Override
  public boolean createPath( String path )
    {
    FileSystem fileSystem = getFileSystem( getSystemConfig(), path );

    try
      {
      return fileSystem.mkdirs( new Path( path ) );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to make path: " + path, exception );
      }
    }

  @Override
  public boolean deletePath( String path )
    {
    FileSystem fileSystem = getFileSystem( getSystemConfig() , path );

    try
      {
      return fileSystem.delete( new Path( path ), true );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to delete path: " + path, exception );
      }
    }

  @Override
  public String getTempPath()
    {
    return Hfs.getTempPath( getSystemConfig() ).toString();
    }

  @Override
  public String getFullPath( String identifier )
    {
    if( identifier == null || identifier.isEmpty() )
      return null;

    // allows us to get the actual case for the path
    FileSystem fileSystem = getFileSystem( getSystemConfig(), identifier );
    Path path = fileSystem.makeQualified( new Path( identifier ) );

    return findActualPath( path.getParent().toString(), path.toString() );
    }

  @Override
  public boolean pathExists( String path )
    {
    FileSystem fileSystem = getFileSystem( getSystemConfig(), path );

    try
      {
      return fileSystem.exists( new Path( path ) );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to test path: " + path, exception );
      }
    }

  @Override
  public InputStream getInputStream( String path )
    {
    if( path == null || path.isEmpty() )
      return null;

    FileSystem fileSystem = getFileSystem( getSystemConfig(), path );

    try
      {
      if( !fileSystem.exists( new Path( path ) ) )
        return null;

      return fileSystem.open( new Path( path ) );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to open path: " + path, exception );
      }
    }

  @Override
  public OutputStream getOutputStream( String stringPath )
    {
    if( stringPath == null || stringPath.isEmpty() )
      return null;

    FileSystem fileSystem = getFileSystem( getSystemConfig(), stringPath );

    try
      {
      Path path = new Path( stringPath );

      if( fileSystem.exists( path ) )
        fileSystem.delete( path, true );
      else
        fileSystem.mkdirs( path.getParent() );

      return fileSystem.create( path );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to open: " + stringPath, exception );
      }
    }

  protected FileSystem getFileSystem( TezConfiguration tezConfiguration, String stringPath )
    {
    URI scheme = makeURIScheme( tezConfiguration, stringPath );

    try
      {
      return FileSystem.get( scheme, tezConfiguration );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to get handle to get filesystem for: " + scheme.getScheme(), exception );
      }
    }

  protected URI makeURIScheme( TezConfiguration tezConfiguration, String stringPath )
    {
    try
      {
      URI uriScheme;

      URI uri = new Path( stringPath ).toUri(); // safer URI parsing
      String schemeString = uri.getScheme();
      String authority = uri.getAuthority();

      if( schemeString != null && authority != null )
        uriScheme = new URI( schemeString + "://" + uri.getAuthority() );
      else if( schemeString != null )
        uriScheme = new URI( schemeString + ":///" );
      else
        uriScheme = getDefaultFileSystemURIScheme( tezConfiguration );

      return uriScheme;
      }
    catch( URISyntaxException exception )
      {
      throw new TapException( "could not determine scheme from path: " + stringPath, exception );
      }
    }

  public URI getDefaultFileSystemURIScheme( TezConfiguration tezConfiguration )
    {
    return getDefaultFileSystem( tezConfiguration ).getUri();
    }

  protected FileSystem getDefaultFileSystem( TezConfiguration tezConfiguration )
    {
    try
      {
      return FileSystem.get( tezConfiguration );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to get handle to underlying filesystem: " + exception.getMessage(), exception );
      }
    }

  @Override
  public String getFileSeparator()
    {
    return "/";
    }
  }
