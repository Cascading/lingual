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
import java.util.Collection;

import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class ValidateProviderCLITest extends CLIPlatformTestCase
  {
  @Ignore
  @Test
  public void testValidateProviderCLI() throws IOException
    {
    initCatalog();

    SchemaCatalog schemaCatalog = getSchemaCatalog();
    Collection<String> providerNames = schemaCatalog.getProviderNames();
    int initialSize = providerNames.size();

    // validate a jar provider
    catalog( "--provider", "jarprovider", "--validate", "--jar", TEST_PROPERTIES_EXTENDS_LOCATION );
    // validate a maven provider
    // TODO: maven install not currently supported.
    //executeCatalog( outputPath, "--provider", "mavenprovider", "--validate", "--spec", "maven:sucks:666" );

    // intentionally fail
    executeCatalogWithOptionalTest( false, "--provider", "jarprovider", "--validate", "--jar", "build/resources/test/jar/not-found-provider.jar" );

    // confirm that validate doesn't add any providers
    providerNames = schemaCatalog.getProviderNames();
    int finalSize = providerNames.size();
    assertEquals( "provider list should not have changed size", initialSize, finalSize );
    }
  }
