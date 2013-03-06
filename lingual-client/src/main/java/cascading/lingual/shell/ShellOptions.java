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
  private final OptionSpec<String> flowPlanPath;
  private final OptionSpec<String> sqlPlanPath;
  private final OptionSpec<String> sqlFile;
  private final OptionSpec<Integer> maxRows;

  public ShellOptions()
    {
    schema = parser.accepts( "schema", "name of current schema" )
      .withRequiredArg();

    schemas = parser.accepts( "schemas", "platform path for each schema to use" )
      .withRequiredArg().withValuesSeparatedBy( ',' );

    resultPath = parser.accepts( "resultPath", "platform path to store results of SELECT queries" )
      .withOptionalArg().defaultsTo( "results" ).describedAs( "directory" );

    flowPlanPath = parser.accepts( "flowPlanPath", "platform path to write flow plan files" )
      .withOptionalArg().defaultsTo( "flowPlanFiles" ).describedAs( "directory" );

    sqlPlanPath = parser.accepts( "sqlPlanPath", "platform path to write SQL plan files" )
      .withOptionalArg().defaultsTo( "sqlPlanPath" ).describedAs( "directory" );

    sqlFile = parser.accepts( "sql", "file with sql commands to execute" )
      .withRequiredArg().describedAs( "filename" );

    maxRows = parser.accepts( "maxRows", "number of results to limit. 0 for unlimited" )
      .withRequiredArg().ofType( Integer.TYPE ).defaultsTo( 10000 ).describedAs( "number of records" );
    }

  public String createJDBCUrl()
    {
    StringBuilder builder = new StringBuilder( "jdbc:lingual:" );

    builder.append( getPlatform() );

    if( getSchema() != null )
      builder.append( ":" ).append( getSchema() );

    if( !getSchemas().isEmpty() )
      {
      builder
        .append( ";" ).append( Driver.SCHEMAS_PROP ).append( "=" )
        .append( Util.join( getSchemas(), "," ) );
      }

    if( getResultPath() != null ) // always override
      {
      builder
        .append( ";" ).append( Driver.RESULT_PATH_PROP ).append( "=" )
        .append( getResultPath() );
      }

    if( getMaxRows() != 0 ) // always override
      {
      builder
        .append( ";" ).append( Driver.MAX_ROWS ).append( "=" )
        .append( getMaxRows() );
      }

    if( isFlowPlanPath() && getFlowPlanPath() != null )
      {
      builder
        .append( ";" ).append( Driver.FLOW_PLAN_PATH ).append( "=" )
        .append( getFlowPlanPath() );
      }

    if( isSQLPlanPath() && getSQLPlanPath() != null )
      {
      builder
        .append( ";" ).append( Driver.SQL_PLAN_PATH_PROP ).append( "=" )
        .append( getSQLPlanPath() );
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

  public boolean isFlowPlanPath()
    {
    return optionSet.has( flowPlanPath );
    }

  public String getFlowPlanPath()
    {
    return optionSet.valueOf( flowPlanPath );
    }

  public boolean isSQLPlanPath()
    {
    return optionSet.has( sqlPlanPath );
    }

  public String getSQLPlanPath()
    {
    return optionSet.valueOf( sqlPlanPath );
    }

  public String getSqlFile()
    {
    return optionSet.valueOf( sqlFile );
    }

  public int getMaxRows()
    {
    return optionSet.valueOf( maxRows );
    }

  /////

  }
