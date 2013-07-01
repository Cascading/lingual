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

package cascading.lingual.catalog.target;

import java.util.Collection;
import java.util.List;

import cascading.lingual.catalog.CatalogOptions;
import cascading.lingual.catalog.Repo;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.common.Printer;
import cascading.lingual.platform.PlatformBroker;

import static java.util.Arrays.asList;

/**
 *
 */
public class RepoTarget extends CRUDTarget
  {
  public RepoTarget( Printer printer, CatalogOptions options )
    {
    super( printer, options );
    }

  @Override
  protected List<String> performAdd( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    String repoName = getOptions().getRepoName();
    String repoUrl = getOptions().getAddURI();

    if( repoName == null || repoUrl == null )
      throw new IllegalArgumentException( "repo add action must have a repo name and a url" );

    Repo repo = new Repo( repoName, repoUrl );

    catalog.addRepo( repo );

    return asList( repoName );
    }

  @Override
  protected boolean performRename( PlatformBroker platformBroker )
    {
    // remove is not supported. There aren't likely to be enough registered repos to make
    // it worth doing, particularly when we don't want people to be able to rename mavencentral
    // or mavenlocal easily. People who need to change a name can do it via remove and an add.
    return false;
    }

  @Override
  protected boolean performRemove( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    String repoName = getOptions().getRepoName();

    if( repoName == null )
      throw new IllegalArgumentException( "remove action must have a name" );

    catalog.removeMavenRepo( repoName );

    return true;
    }

  @Override
  protected boolean performValidateDependencies( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    String repoName = getOptions().getRepoName();

    if( repoName == null )
      throw new IllegalArgumentException( "validate action must have a name" );

    Repo repo = catalog.getMavenRepo( repoName );

    // TODO: validate the repo exists and that downloading the spec gets a workable jar.
    return true;
    }

  @Override
  protected Collection<String> performGetNames( PlatformBroker platformBroker )
    {
    return platformBroker.getCatalog().getMavenRepoNames();
    }
  }
