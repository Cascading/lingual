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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Properties;

import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.platform.PlatformBrokerFactory;
import com.google.common.base.Joiner;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class ProviderCLITest extends CLIPlatformTestCase
  {
  @Ignore
  @Test
  public void testProviderCLI() throws IOException
    {
    createProviderJar( TEST_PROPERTIES_EXTENDS_LOCATION, null );

    String catalogPath = getCatalogPath();

    initCatalog();

    // there should be no providers to start with.
    SchemaCatalog schemaCatalog = getSchemaCatalog();
    Collection<String> providerNames = schemaCatalog.getProviderNames();
    int initialProviderCount = providerNames.size();

    // add in a provider at the root level
    catalog( "--provider", "jarprovider", "--add", "--jar", getProviderPath() );
    providerNames = schemaCatalog.getProviderNames();
    assertTrue( "On jar provider add, jarprovider not found in " + providerNames.toString(), providerNames.contains( "jarprovider" ) );

    // test that we actually installed the file
    String expectedPath = Joiner.on( File.separator ).join( catalogPath, TEST_META_DATA_PATH_PROP, "providers", TEST_PROVIDER_JAR_NAME );
    validateInstalledJar( expectedPath, catalogPath );

    // add in a providers at schema level
    catalog( "--schema", "providerschema", "--add" );
    catalog( "--schema", "providerschema", "--provider", "testsubprovider", "--add", "--jar", getProviderPath() );
    // schema should contain providers
    providerNames = schemaCatalog.getProviderNames( "providerschema" );
    assertTrue( "On schema add, testsubprovider not found in schema " + providerNames.toString(), providerNames.contains( "testsubprovider" ) );
    ProviderDef provider = schemaCatalog.getSchemaDef( "providerschema" ).getProviderDef( "testsubprovider" );
    assertNotNull( "sub-schema did not contain provider", provider );

    // should now have 1 more providers registered
    providerNames = schemaCatalog.getProviderNames();
    int currentProviderCount = providerNames.size() - initialProviderCount;
    assertEquals( "after adds wrong # of values added to " + providerNames.toString(), 1, currentProviderCount );

    // remove a provider
    catalog( "--provider", "jarprovider", "--remove" );
    providerNames = schemaCatalog.getProviderNames();
    assertFalse( "On schema remove, jarprovider still found", providerNames.contains( "jarprovider" ) );

    // ****
    // now do error case handling
    // ****

    try
      {
      executeCatalogWithOptionalTest( false, "--provider", "testsubprovider", "--add" );
      }
    catch( Exception e )
      {
      assertEquals( "testing for missing params", "either jar or spec is required to add a provider", e.getMessage() );
      }

    try
      {
      executeCatalogWithOptionalTest( false, "--provider", "testsubprovider", "--add", "--spec", "maven:sucks:666", "--jar", "file://tmp/bogus.jar" );
      }
    catch( Exception e )
      {
      assertEquals( "testing for dupe params", "only one of [spec,jar] is allowed when adding a provider", e.getMessage() );
      }

    // confirm that the basic list command doesn't err.
    catalog( "--provider" );
    }

  private void validateInstalledJar( String expectedPath, String outputPath )
    {
    boolean fileExists = false;
    long fileLength = 0;
    if( "local".equals( getPlatform().getName() ) )
      {
      // manually test we put the file in the location to confirm that the
      // platform broker did what we wanted it to when run locally.
      File expectedInstallLoctaion = new File( expectedPath );
      fileExists = expectedInstallLoctaion.exists();
      fileLength = expectedInstallLoctaion.length();
      }
    else if( "hadoop".equals( getPlatform().getName() ) )
      {
      // rely on the the platform broker to test this.
      Properties platformProperties = getPlatformProperties( outputPath );
      PlatformBroker platformBroker = PlatformBrokerFactory.createPlatformBroker( getPlatformName(), platformProperties );
      InputStream inputStream = new BufferedInputStream( platformBroker.getInputStream( expectedPath ) );
      fileExists = inputStream != null; // hadoop broker returns null if file not found.
      try
        {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int next = inputStream.read();
        while( next > -1 )
          {
          bos.write( next );
          next = inputStream.read();
          }
        bos.flush();
        byte[] result = bos.toByteArray();
        // NullPointer would get thrown if the inputStream isn't valid so:
        fileLength = result.length;
        }
      catch( IOException e )
        {
        fail( "Error reading bytes from hadoop platform from " + expectedPath + " because " + e.getMessage() );
        e.printStackTrace();
        }
      catch( NullPointerException e )
        {
        fail( "No data found at " + expectedPath );
        }
      }
    else
      {
      fail( "Do not know how to test jar location from platform:  " + getPlatform().getName() );
      }

    assertTrue( "Jar was not installed where expected: " + expectedPath, fileExists );
    assertTrue( "Jar was installed improperly", fileLength > 0 );
    }

  }
