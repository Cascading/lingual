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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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
import com.google.common.collect.ObjectArrays;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class CLIPlatformTestCase extends LingualPlatformTestCase
  {
  private static final Logger LOG = LoggerFactory.getLogger( CLIPlatformTestCase.class );

  public static final String TEST_META_DATA_PATH_PROP = "_lingual";
  public static final String TEST_PROVIDER_PROPERTIES = "provider.properties";
  public static final String TEST_PROVIDER_JAR_NAME = "pipeprovider.jar";
  public static final String TEST_PROPERTIES_LOCATION = PROVIDER_PATH + TEST_PROVIDER_PROPERTIES;
  public static final String BUILD_TEST_PROVIDER = "build/test/provider/";
  public static final String PROVIDER_JAR_PATH = BUILD_TEST_PROVIDER + TEST_PROVIDER_JAR_NAME;

  @Before
  public void before()
    {
    System.setProperty( "sqlline.system.exit", "true" ); // goofy logic, but true prevents exit from being called
    Logging.setLogLevel( "debug" );
    }

  protected void createProviderJar() throws IOException
    {
    File[] contents = new File[]{new File( TEST_PROPERTIES_LOCATION )};
    File jarFile = new File( PROVIDER_JAR_PATH );

    jarFile.getParentFile().mkdirs();

    if( jarFile.exists() )
      jarFile.delete();

    byte buffer[] = new byte[ 10240 ];
    FileOutputStream stream = new FileOutputStream( jarFile );
    JarOutputStream out = new JarOutputStream( stream, new Manifest() );

    File packagePath = new File( "cascading/bind/" );

    for( File file : contents )
      {
      if( file == null || !file.exists() || file.isDirectory() )
        continue;

      String currentContent = new File( packagePath, file.getName() ).toString();

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
    }

  protected void initCatalog() throws IOException
    {
    getPlatform().remoteRemove( getCatalogPath(), true );
    catalog( "--init" );
    }

  protected SchemaCatalog getSchemaCatalog()
    {
    Properties platformProperties = getPlatformProperties( getCatalogPath() );
    PlatformBroker platformBroker = PlatformBrokerFactory.createPlatformBroker( getPlatformName(), platformProperties );

    return platformBroker.getCatalog();
    }

  protected void catalog( String... args ) throws IOException
    {
    executeCatalogWithOptionalTest( true, args );
    }

  protected void executeCatalogWithOptionalTest( boolean expectedResult, String... args ) throws IOException
    {
    args = ObjectArrays.concat( new String[]{"--verbose", "debug"}, args, String.class );
    boolean result = createCatalog( getCatalogPath() ).execute( args );
    assertEquals( "executeCatalog returned false", expectedResult, result );
    }

  protected boolean shell( String... args ) throws IOException
    {
    args = ObjectArrays.concat( new String[]{"--verbose", "debug"}, args, String.class );
    Shell shell = createShell( getCatalogPath() );
    return shell.execute( args );
    }

  protected Catalog createCatalog( String catalogPath )
    {
    Properties platformProperties = getPlatformProperties( catalogPath );
    return new Catalog( System.out, System.err, platformProperties );
    }

  private Shell createShell( String catalogPath )
    {
    Properties platformProperties = getPlatformProperties( catalogPath );
    return new Shell( System.out, System.err, platformProperties );
    }

  protected Properties getPlatformProperties( String rootPath )
    {
    Properties properties = new Properties();

    properties.setProperty( Driver.CATALOG_PROP, rootPath );
    properties.setProperty( PlatformBroker.META_DATA_DIR_NAME_PROP, TEST_META_DATA_PATH_PROP );
    properties.setProperty( PlatformBroker.CATALOG_FILE_NAME_PROP, "catalog.json" );
    properties.setProperty( PlatformBrokerFactory.PLATFORM_NAME, getPlatformName() );

    return properties;
    }
  }
