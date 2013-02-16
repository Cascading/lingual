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

package cascading.lingual.platform.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.platform.PlatformBroker;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.TapException;
import cascading.tap.hadoop.Hfs;
import cascading.tap.type.FileType;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HadoopPlatformBroker extends PlatformBroker<JobConf>
  {
  private static final Logger LOG = LoggerFactory.getLogger( HadoopPlatformBroker.class );

  public static final String HADOOP_USER_ENV = "HADOOP_USER_NAME";
  public static final String HADOOP_USER_PROPERTY = "hadoop.username";
  private JobConf jobConf;

  public HadoopPlatformBroker()
    {
    }

  @Override
  public String getName()
    {
    return "hadoop";
    }

  @Override
  public JobConf getConfig()
    {
    if( jobConf != null )
      return jobConf;

    jobConf = HadoopUtil.createJobConf( getProperties(), new JobConf() );

    String appJar = findAppJar();

    if( jobConf.getJar() == null && appJar != null )
      jobConf.setJar( appJar );

    String userName = findUserName();

    if( jobConf.getUser() == null && userName != null )
      jobConf.setUser( userName );

    LOG.info( "using app jar: {}", jobConf.getJar() );
    LOG.info( "using user: {}", jobConf.getUser() );

    URL url = Thread.currentThread().getContextClassLoader().getResource( "hadoop-override.properties" );

    if( url != null )
      {
      LOG.info( "loading override properties from: {}", url.toString() );

      Properties properties = loadPropertiesFrom( url );

      for( String propertyName : properties.stringPropertyNames() )
        jobConf.set( propertyName, properties.getProperty( propertyName ) );

      if( LOG.isDebugEnabled() )
        LOG.debug( HadoopUtil.createProperties( jobConf ).toString() );
      }

    return jobConf;
    }

  private Properties loadPropertiesFrom( URL url )
    {
    Properties properties = new Properties();

    try
      {
      properties.load( url.openStream() );
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to open resource file" );
      }

    return properties;
    }

  private String findAppJar()
    {
    URL url = Thread.currentThread().getContextClassLoader().getResource( "META-INF/hadoop.job.properties" );

    if( url == null || !url.toString().startsWith( "jar" ) )
      return null;

    String jarPath;

    if( !"jar".equals( url.getProtocol() ) )
      throw new RuntimeException( "invalid url: " + url.toString() );

    jarPath = url.getPath();

    if( jarPath.startsWith( "file:" ) )
      jarPath = jarPath.substring( "file:".length() );

    jarPath = decode( jarPath );

    jarPath = jarPath.replaceAll( "!.*$", "" );

    LOG.info( "using hadoop job jar: {}", jarPath );

    return jarPath;
    }

  private String decode( String jarPath )
    {
    try
      {
      return URLDecoder.decode( jarPath, "UTF-8" );
      }
    catch( UnsupportedEncodingException exception )
      {
      throw new RuntimeException( exception.getMessage(), exception );
      }
    }

  private String findUserName()
    {
    return System.getProperty( HADOOP_USER_PROPERTY, System.getenv( HADOOP_USER_ENV ) );
    }

  @Override
  public FlowConnector getFlowConnector()
    {
    return new HadoopFlowConnector( HadoopUtil.createProperties( getConfig() ) );
    }

  @Override
  public FlowProcess<JobConf> getFlowProcess()
    {
    return new HadoopFlowProcess( getConfig() );
    }

  @Override
  public Class<? extends SchemaCatalog> getCatalogClass()
    {
    return HadoopCatalog.class;
    }

  @Override
  public FileType getFileTypeFor( String identifier )
    {
    return new Hfs( new TextLine(), identifier, SinkMode.KEEP );
    }

  @Override
  public String[] getChildIdentifiers( FileType<JobConf> fileType ) throws IOException
    {
    return fileType.getChildIdentifiers( getConfig() );
    }

  @Override
  public boolean createPath( String path )
    {
    FileSystem fileSystem = getFileSystem( getConfig(), path );

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
    FileSystem fileSystem = getFileSystem( getConfig(), path );

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
    return Hfs.getTempPath( getConfig() ).toString();
    }

  @Override
  public String getFullPath( String identifier )
    {
    if( identifier == null || identifier.isEmpty() )
      return null;

    FileSystem fileSystem = getFileSystem( getConfig(), identifier );

    return fileSystem.makeQualified( new Path( identifier ) ).toString();
    }

  @Override
  public boolean pathExists( String path )
    {
    FileSystem fileSystem = getFileSystem( getConfig(), path );

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
  protected InputStream getInputStream( String path )
    {
    if( path == null || path.isEmpty() )
      return null;

    FileSystem fileSystem = getFileSystem( getConfig(), path );

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
  protected OutputStream getOutputStream( String stringPath )
    {
    if( stringPath == null || stringPath.isEmpty() )
      return null;

    FileSystem fileSystem = getFileSystem( getConfig(), stringPath );

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
      throw new RuntimeException( "unable to open stringPath: " + stringPath, exception );
      }
    }

  protected FileSystem getFileSystem( JobConf jobConf, String stringPath )
    {
    URI scheme = makeURIScheme( jobConf, stringPath );

    try
      {
      return FileSystem.get( scheme, jobConf );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to get handle to get filesystem for: " + scheme.getScheme(), exception );
      }
    }

  protected URI makeURIScheme( JobConf jobConf, String stringPath )
    {
    try
      {
      URI uriScheme;

      LOG.debug( "handling path: {}", stringPath );

      URI uri = new Path( stringPath ).toUri(); // safer URI parsing
      String schemeString = uri.getScheme();
      String authority = uri.getAuthority();

      if( LOG.isDebugEnabled() )
        {
        LOG.debug( "found scheme: {}", schemeString );
        LOG.debug( "found authority: {}", authority );
        }

      if( schemeString != null && authority != null )
        uriScheme = new URI( schemeString + "://" + uri.getAuthority() );
      else if( schemeString != null )
        uriScheme = new URI( schemeString + ":///" );
      else
        uriScheme = getDefaultFileSystemURIScheme( jobConf );

      LOG.debug( "using uri scheme: {}", uriScheme );

      return uriScheme;
      }
    catch( URISyntaxException exception )
      {
      throw new TapException( "could not determine scheme from path: " + stringPath, exception );
      }
    }

  public URI getDefaultFileSystemURIScheme( JobConf jobConf )
    {
    return getDefaultFileSystem( jobConf ).getUri();
    }

  protected FileSystem getDefaultFileSystem( JobConf jobConf )
    {
    try
      {
      return FileSystem.get( jobConf );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to get handle to underlying filesystem", exception );
      }
    }

  protected String getFileSeparator()
    {
    return "/";
    }
  }
