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
public class ValidateRepoCLITest extends CLIPlatformTestCase
  {
  @Ignore
  @Test
  public void testValidateRepoCLI() throws IOException
    {
    initCatalog();

    SchemaCatalog schemaCatalog = getSchemaCatalog();
    Collection<String> mavenRepoNames = schemaCatalog.getMavenRepoNames();
    int initialSize = mavenRepoNames.size();

    catalog( "--repo", "conjars", "--validate", "--url", "http://foo.root.com" );

    // confirm that validate doesn't add any any repos
    mavenRepoNames = schemaCatalog.getMavenRepoNames();
    int finalSize = mavenRepoNames.size();
    assertEquals( "repo list should not have changed size", initialSize, finalSize );
    }
  }
