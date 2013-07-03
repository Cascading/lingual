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

import org.junit.Test;

/**
 *
 */
public class RepoCLITest extends CLIPlatformTestCase
  {
  @Test
  public void testRepoCLI() throws IOException
    {
    initCatalog();

    // check we start with default repos.
    SchemaCatalog schemaCatalog = getSchemaCatalog();
    Collection<String> mavenRepoNames = schemaCatalog.getMavenRepoNames();
    assertEquals( "values found in " + mavenRepoNames.toString(), 3, mavenRepoNames.size() );
    assertTrue( "missing mavencentral", mavenRepoNames.contains( "mavencentral" ) );
    assertTrue( "missing mavenlocal", mavenRepoNames.contains( "mavenlocal" ) );
    assertTrue( "missing conjars", mavenRepoNames.contains( "conjars" ) );

    // add in a repo at root level
    catalog( "--repo", "testrootrepo", "--add", "http://foo.root.com" );
    mavenRepoNames = schemaCatalog.getMavenRepoNames();
    assertTrue( "On root add, testrootrepo not found in " + mavenRepoNames.toString(), mavenRepoNames.contains( "testrootrepo" ) );

    // remove a repo
    catalog( "--repo", "testrootrepo", "--remove" );
    // should be gone
    mavenRepoNames = schemaCatalog.getMavenRepoNames();
    assertFalse( "On root remove, testrootrepo still found in " + mavenRepoNames.toString(), mavenRepoNames.contains( "testrootrepo" ) );
    // should still have the default repos
    assertEquals( "On root remove, wrong # of repos found in " + mavenRepoNames.toString(), 3, mavenRepoNames.size() );

    // confirm that the basic list command doesn't err.
    catalog( "--repo" );
    }
  }
