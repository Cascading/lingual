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

package cascading.lingual.catalog;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import cascading.lingual.LingualPlatformTestCase;
import cascading.lingual.jdbc.Driver;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.platform.PlatformBrokerFactory;
import cascading.lingual.shell.Shell;
import cascading.lingual.util.Logging;
import cascading.lingual.util.Misc;
import com.google.common.collect.ObjectArrays;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Joiner.on;
import static com.google.common.collect.Maps.fromProperties;

/**
 *
 */
public abstract class CLIPlatformTestCase extends LingualPlatformTestCase
  {
  private static final Logger LOG = LoggerFactory.getLogger( CLIPlatformTestCase.class );

  public static final String TEST_META_DATA_PATH_PROP = "_lingual";
  public static final String TEST_PROVIDER_PROPERTIES_EXTENDS = "extends/provider.properties";
  public static final String TEST_PROVIDER_PROPERTIES_FACTORY = "factory/provider.properties";
  public static final String TEST_PROVIDER_POM = "pom/pom.xml";
  public static final String TEST_PROVIDER_JAR_NAME = "pipeprovider.jar";

  public static final String TEST_PROPERTIES_EXTENDS_LOCATION = PROVIDER_PATH + TEST_PROVIDER_PROPERTIES_EXTENDS;
  public static final String TEST_PROPERTIES_FACTORY_LOCATION = PROVIDER_PATH + TEST_PROVIDER_PROPERTIES_FACTORY;
  public static final String TEST_PROPERTIES_POM = PROVIDER_PATH + TEST_PROVIDER_POM;

  protected CLIPlatformTestCase( boolean useCluster )
    {
    super( useCluster );
    }

  protected CLIPlatformTestCase()
    {
    }

  @Before
  public void before()
    {
    System.setProperty( "sqlline.system.exit", "true" ); // goofy logic, but true prevents exit from being called
    Logging.setLogLevel( "debug" );
    }

  protected String getProviderPath( String testProviderJarName )
    {
    return getRootPath() + "/provider/" + getTestName() + "/" + testProviderJarName;
    }

  protected String getTablePath()
    {
    return getRootPath() + "/table/" + getTestName() + "/result.tcsv";
    }

  protected String getSchemaPath( String schemaName )
    {
    return getRootPath() + "/table/" + getTestName() + "/" + schemaName;
    }

  protected String getFactoryPath()
    {
    return getRootPath() + "/factory/" + getTestName() + "/";
    }

  protected String createProviderJar( String propertiesPath, Collection<File> classPath, String providerPath ) throws IOException
    {
    List<File> contents = new ArrayList<File>();
    List<File> locations = new ArrayList<File>();

    contents.add( new File( propertiesPath ) );
    locations.add( new File( "cascading/bind/" ) );

    for( File file : classPath )
      {
      contents.add( file );
      locations.add( new File( "lingual/test/" ) );
      }

    File jarFile = new File( providerPath );

    jarFile.getParentFile().mkdirs();

    if( jarFile.exists() )
      jarFile.delete();

    byte buffer[] = new byte[ 10240 ];
    FileOutputStream stream = new FileOutputStream( jarFile );
    JarOutputStream out = new JarOutputStream( stream, new Manifest() );

    for( int i = 0; i < contents.size(); i++ )
      {
      File file = contents.get( i );

      if( file == null || !file.exists() || file.isDirectory() )
        continue;

      String currentContent = new File( locations.get( i ), file.getName() ).toString();

      LOG.debug( "adding " + currentContent );

      JarEntry jarEntry = new JarEntry( currentContent );

      jarEntry.setTime( file.lastModified() );
      out.putNextEntry( jarEntry );

      FileInputStream in = new FileInputStream( file );

      while( true )
        {
        int nRead = in.read( buffer, 0, buffer.length );

        if( nRead <= 0 )
          break;

        out.write( buffer, 0, nRead );
        }

      in.close();
      }

    out.close();
    stream.close();
    LOG.debug( "adding completed OK" );
    return Misc.getHash( jarFile );
    }

  protected void initCatalog() throws IOException
    {
    PlatformBrokerFactory.instance().reloadBrokers();
    getPlatform().remoteRemove( getCatalogPath(), true );
    catalog( "--init" );
    getSchemaCatalog();
    }

  protected SchemaCatalog getSchemaCatalog()
    {
    Properties platformProperties = getPlatformProperties();
    PlatformBroker platformBroker = PlatformBrokerFactory.createPlatformBroker( getPlatformName(), platformProperties );

    return platformBroker.getCatalog();
    }

  protected void catalog( String... args ) throws IOException
    {
    catalog( true, args );
    }

  protected void catalog( boolean expectedResult, String... args ) throws IOException
    {
    args = ObjectArrays.concat( new String[]{"--verbose", "debug"}, args, String.class );
    boolean result = createCatalog().execute( args );
    assertEquals( "'catalog " + Arrays.deepToString( args ) + "' returned incorrect status ", expectedResult, result );
    }

  protected void shellSQL( String sql ) throws IOException
    {
    shellSQL( true, sql );
    }

  protected void shellSQL( boolean expectedResult, String sql ) throws IOException
    {
    String[] args = new String[]{"--verbose", "debug", "--sql", "-", "--platform", getPlatformName(),
                                 "--resultPath", getResultPath()};
    Shell shell = createShell( new ByteArrayInputStream( sql.concat( "\n" ).getBytes() ) );
    boolean result = shell.execute( args );
    assertEquals( "'" + sql + "' returned incorrect status", expectedResult, result );
    }

  protected Catalog createCatalog()
    {
    Properties platformProperties = getPlatformProperties();
    return new Catalog( System.out, System.err, platformProperties );
    }

  private Shell createShell( InputStream inputStream )
    {
    Properties platformProperties = getPlatformProperties();
    return new Shell( inputStream, System.out, System.err, platformProperties );
    }

  protected Properties getPlatformProperties()
    {
    Properties properties = new Properties();

    properties.putAll( getProperties() ); // add platform properties

    properties.setProperty( Driver.CATALOG_PROP, getCatalogPath() );
    properties.setProperty( Driver.RESULT_PATH_PROP, getResultPath() );
    properties.setProperty( PlatformBroker.META_DATA_DIR_NAME_PROP, TEST_META_DATA_PATH_PROP );
    properties.setProperty( PlatformBroker.CATALOG_FILE_NAME_PROP, "catalog.json" );
    properties.setProperty( PlatformBrokerFactory.PLATFORM_NAME, getPlatformName() );

    String join = on( ";" ).withKeyValueSeparator( "=" ).join( fromProperties( properties ) );

    properties.setProperty( "urlProperties", join );

    return properties;
    }
  }
