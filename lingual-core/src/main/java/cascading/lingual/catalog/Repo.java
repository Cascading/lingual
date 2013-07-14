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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Hack class wrapper for a basic string to deal with Jackson's problems parsing a String in a typed collection.
 * See http://jira.codehaus.org/browse/JACKSON-708 for the bug.
 */
public class Repo
  {
  public static final Repo MAVEN_CENTRAL = new Repo( Kind.Maven2, "mavencentral", "http://repo1.maven.org/maven2/" );
  public static final Repo MAVEN_LOCAL = new Repo( Kind.Maven2, "mavenlocal", System.getenv( "HOME" ) + File.separator + ".m2/" );
  public static final Repo MAVEN_CONJARS = new Repo( Kind.Maven2, "conjars", "http://conjars.org/repo/" );

  public enum Kind
    {
      Maven2, Ivy
    }

  @JsonProperty
  private Kind repoKind;

  @JsonProperty
  private String repoName;

  @JsonProperty
  private String repoUrl;

  private Repo()
    {
    }

  public Repo( Kind repoKind, String repoName, String repoUrl )
    {
    this.repoKind = repoKind;
    this.repoName = repoName;
    this.repoUrl = repoUrl;
    }

  public Repo( String repoName, String repoUrl )
    {
    this.repoKind = Kind.Maven2;
    this.repoName = repoName;
    this.repoUrl = repoUrl;
    }

  public Kind getRepoKind()
    {
    return repoKind;
    }

  public String getRepoName()
    {
    return repoName;
    }

  public String getRepoUrl()
    {
    return repoUrl;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Repo repo = (Repo) object;

    if( repoKind != repo.repoKind )
      return false;
    if( repoName != null ? !repoName.equals( repo.repoName ) : repo.repoName != null )
      return false;
    if( repoUrl != null ? !repoUrl.equals( repo.repoUrl ) : repo.repoUrl != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = repoKind != null ? repoKind.hashCode() : 0;
    result = 31 * result + ( repoName != null ? repoName.hashCode() : 0 );
    result = 31 * result + ( repoUrl != null ? repoUrl.hashCode() : 0 );
    return result;
    }
  }
