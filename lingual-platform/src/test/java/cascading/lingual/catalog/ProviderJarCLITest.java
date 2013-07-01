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

import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class ProviderJarCLITest extends CLIPlatformTestCase
  {

  private static final String PROVIDER_SQL_SELECT_FILE = QUERY_FILES_PATH + "provider_select.sql";
  private static final String TEST_PROVIDER_JAR_NAME = "pipeprovider.jar";
  public static final String PIPE_PROVIDER_JAR = PROVIDER_PATH + TEST_PROVIDER_JAR_NAME;

  @Ignore
  @Test
  public void testProviderWithSQLLine() throws IOException
    {
    createProviderJar();

    initCatalog();

    catalog( "--provider", "--add", PIPE_PROVIDER_JAR );

    SchemaCatalog schemaCatalog = getSchemaCatalog();
    Format format = Format.getFormat( "bin" );
    ProviderDef providerDef = schemaCatalog.findProviderDefFor( null, format );
    assertNotNull( "provider not registered to format", providerDef );

    Protocol protocol = Protocol.getProtocol( "memcached-binary" );
    schemaCatalog = getSchemaCatalog();
    providerDef = schemaCatalog.findProviderDefFor( null, protocol );
    assertNotNull( "provider not registered to protocol", providerDef );


    // now confirm that reading this from sqlline
    assertTrue( "unable to run query", shell( "--sql", PROVIDER_SQL_SELECT_FILE, "--provider", getPlatformName() ) );
    }

  }
