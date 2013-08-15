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

import java.io.File;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import cascading.lingual.catalog.CatalogOptions;
import cascading.lingual.catalog.Repo;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.builder.RepoBuilder;
import cascading.lingual.common.Printer;
import cascading.lingual.platform.PlatformBroker;
import org.apache.ivy.Ivy;
import org.apache.ivy.core.settings.IvySettings;
import org.apache.ivy.plugins.resolver.IBiblioResolver;
import org.apache.ivy.plugins.resolver.RepositoryResolver;

import static java.util.Arrays.asList;

/**
 *
 */
public class RepoTarget extends CRUDTarget
  {
  private static final String M2_PER_MODULE_PATTERN = "[revision]/[artifact]-[revision](-[classifier]).[ext]";
  private static final String M2_PATTERN = "[organisation]/[module]/" + M2_PER_MODULE_PATTERN;

  public RepoTarget( Printer printer, CatalogOptions options )
    {
    super( printer, options );
    }

  @Override
  protected void validateAdd( PlatformBroker platformBroker )
    {
    }

  @Override
  protected List<String> performAdd( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    Repo repo = getRepoFromArgs();
    catalog.addRepo( repo );

    return asList( repo.getRepoName() );
    }

  @Override
  protected boolean performRename( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    return catalog.renameMavenRepo( getOptions().getRepoName(), getOptions().getRenameName() );
    }

  @Override
  protected Object getSource( PlatformBroker platformBroker )
    {
    return platformBroker.getCatalog().getMavenRepo( getOptions().getRepoName() );
    }

  @Override
  protected String getRequestedSourceName()
    {
    return getOptions().getRepoName();
    }

  @Override
  protected boolean performRemove( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    String repoName = getOptions().getRepoName();

    catalog.removeMavenRepo( repoName );

    return true;
    }

  @Override
  protected boolean performValidateDependencies( PlatformBroker platformBroker )
    {
    RepositoryResolver repositoryResolver = getRepositoryResolver( getRepoFromArgs() );
    IvySettings ivySettings = new IvySettings();
    ivySettings.addResolver( repositoryResolver );
    ivySettings.setDefaultResolver( repositoryResolver.getName() );
    Ivy ivy = Ivy.newInstance( ivySettings );

    return ivy.listOrganisations().length > 0;
    }

  @Override
  protected Collection<String> performGetNames( PlatformBroker platformBroker )
    {
    return platformBroker.getCatalog().getMavenRepoNames();
    }

  @Override
  protected Map performShow( PlatformBroker platformBroker )
    {
    return new RepoBuilder().format( getRepoFromArgs() );
    }

  private Repo getRepoFromArgs()
    {
    String repoName = getOptions().getRepoName();
    String repoUrl = getOptions().getAddURI();

    if( repoName == null )
      throw new IllegalArgumentException( "repo add action must have a repo name" );
    if( repoUrl == null )
      throw new IllegalArgumentException( "repo add action must have an url" );

    return new Repo( repoName, repoUrl );
    }

  protected static RepositoryResolver getRepositoryResolver( Repo repo )
    {
    String repoUrl = repo.getRepoUrl();

    if( !repoUrl.endsWith( "/" ) )
      repoUrl += "/";

    IBiblioResolver resolver = new IBiblioResolver();

    if( URI.create( repoUrl ).getScheme() == null )
      repoUrl = new File( repoUrl ).getAbsoluteFile().toURI().toASCIIString();

    resolver.setM2compatible( repo.getRepoKind() == Repo.Kind.Maven2 );
    resolver.setName( repo.getRepoName() );
    resolver.setRoot( repoUrl );
//    resolver.addArtifactPattern( repoUrl + M2_PATTERN );

    return resolver;
    }

  }
