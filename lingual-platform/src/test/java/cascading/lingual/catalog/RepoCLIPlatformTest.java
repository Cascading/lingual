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
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;

import org.junit.Test;

import static org.junit.Assume.assumeTrue;

/**
 *
 */
public class RepoCLIPlatformTest extends CLIPlatformTestCase
  {

  public static final String PROP_OMIT_REPO_NETWORK_TEST = "lingual.test.repo.network.omit";

  @Test
  public void testValidateRepoCLI() throws IOException
    {
    // since the repo validation is a network event the unit test makes a network call
    // this might be an issue for people developing remotely on laptops so allow dynamic ignoring
    assumeTrue( "Test ignored since user set -D" + PROP_OMIT_REPO_NETWORK_TEST, System.getProperty( PROP_OMIT_REPO_NETWORK_TEST ) == null );

    try
      {
      URL conjarsUrl = new URL( "http://conjars.org" );
      HttpURLConnection urlConnection = (HttpURLConnection) conjarsUrl.openConnection();
      urlConnection.connect();
      urlConnection.disconnect();
      }
    catch( IOException ioException )
      {
      fail( "this machine needs network access to conjars.org to do this test. to omit this test without changing code, set -D" + PROP_OMIT_REPO_NETWORK_TEST );
      }

    initCatalog();
    int initialSize = getSchemaCatalog().getRepoNames().size();

    // a legit repo that should pass
    catalog( "--repo", "conjars", "--validate", "--add", "http://conjars.org/repo" );

    // this is not a valid repo and should fail.
    catalog( false, "--repo", "conjars", "--validate", "--add", "http://conjars.org/not_a_valid_repo/" );

    // confirm that validate doesn't add any any repos
    int finalSize = getSchemaCatalog().getRepoNames().size();
    assertEquals( "repo list should not have changed size", initialSize, finalSize );

    }

  @Test
  public void testRepoCLI() throws IOException
    {
    initCatalog();

    // check we start with default repos.
    SchemaCatalog schemaCatalog = getSchemaCatalog();
    Collection<String> mavenRepoNames = schemaCatalog.getRepoNames();
    assertEquals( "values found in " + mavenRepoNames.toString(), 3, mavenRepoNames.size() );
    assertTrue( "missing mavencentral", mavenRepoNames.contains( "mavencentral" ) );
    assertTrue( "missing mavenlocal", mavenRepoNames.contains( "mavenlocal" ) );
    assertTrue( "missing conjars", mavenRepoNames.contains( "conjars" ) );

    // add in a repo at root level
    catalog( "--repo", "testrootrepo", "--add", "http://foo.root.com" );
    mavenRepoNames = schemaCatalog.getRepoNames();
    assertTrue( "On root add, testrootrepo not found in " + mavenRepoNames.toString(), mavenRepoNames.contains( "testrootrepo" ) );

    // remove a repo
    catalog( "--repo", "testrootrepo", "--remove" );
    // should be gone
    mavenRepoNames = schemaCatalog.getRepoNames();
    assertFalse( "On root remove, testrootrepo still found in " + mavenRepoNames.toString(), mavenRepoNames.contains( "testrootrepo" ) );
    // should still have the default repos
    assertEquals( "On root remove, wrong # of repos found in " + mavenRepoNames.toString(), 3, mavenRepoNames.size() );

    // confirm that the basic list command doesn't err.
    catalog( "--repo" );
    }
  }
