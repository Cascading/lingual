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

package cascading.lingual.shell;

import java.util.List;

import cascading.lingual.common.Options;
import cascading.lingual.jdbc.Driver;
import cascading.util.Util;
import joptsimple.OptionSpec;

/**
 *
 */
public class ShellOptions extends Options
  {
  private final OptionSpec<String> schema;
  private final OptionSpec<String> schemas;
  private final OptionSpec<String> resultPath;
  private final OptionSpec<String> dotPath;
  private final OptionSpec<String> sqlFile;

  public ShellOptions()
    {
    schema = parser.accepts( "schema", "name of current schema" )
      .withRequiredArg();

    schemas = parser.accepts( "schemas", "root path for each schema to use" )
      .withRequiredArg().withValuesSeparatedBy( ',' );

    resultPath = parser.accepts( "resultPath", "root temp path to store results" )
      .withRequiredArg().describedAs( "directory" );

    dotPath = parser.accepts( "dotPath", "path to write flow dot files" )
      .withRequiredArg().describedAs( "directory" );

    sqlFile = parser.accepts( "sql", "file with sql commands to execute" )
      .withRequiredArg().describedAs( "filename" );
    }

  public String createJDBCUrl()
    {
    StringBuilder builder = new StringBuilder( "jdbc:lingual:" );

    builder.append( getPlatform() );

    if( getSchema() != null )
      {
      builder.append( ":" ).append( getSchema() );
      }

    if( !getSchemas().isEmpty() )
      {
      builder
        .append( ";" ).append( Driver.SCHEMAS_PROP ).append( "=" )
        .append( Util.join( getSchemas(), "," ) );
      }

    if( getResultPath() != null )
      {
      builder
        .append( ";" ).append( Driver.RESULT_PATH_PROP ).append( "=" )
        .append( getResultPath() );
      }

    if( getDotPath() != null )
      {
      builder
        .append( ";" ).append( Driver.DOT_PATH_PROP ).append( "=" )
        .append( getDotPath() );
      }

    if( getSqlFile() != null )
      {
      builder
        .append( ";" ).append( Driver.COLLECTOR_CACHE_PROP ).append( "=true" );
      }

    return builder.toString();
    }

  /////

  public String getSchema()
    {
    return optionSet.valueOf( schema );
    }

  public List<String> getSchemas()
    {
    return optionSet.valuesOf( schemas );
    }

  public String getResultPath()
    {
    return optionSet.valueOf( resultPath );
    }

  public String getDotPath()
    {
    return optionSet.valueOf( dotPath );
    }

  public String getSqlFile()
    {
    return optionSet.valueOf( sqlFile );
    }

  /////

  }
