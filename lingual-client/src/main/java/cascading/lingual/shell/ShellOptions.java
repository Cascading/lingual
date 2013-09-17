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
import java.util.Properties;

import cascading.lingual.common.Options;
import cascading.lingual.jdbc.Driver;
import cascading.util.Util;
import joptsimple.OptionSpec;

/**
 *
 */
public class ShellOptions extends Options
  {
  private final OptionSpec<String> username;
  private final OptionSpec<String> password;
  private final OptionSpec<String> schema;
  private final OptionSpec<String> schemas;
  private final OptionSpec<String> resultSchema;
  private final OptionSpec<String> resultPath;
  private final OptionSpec<String> flowPlanPath;
  private final OptionSpec<String> sqlPlanPath;
  private final OptionSpec<String> sqlFile;
  private final OptionSpec<Integer> maxRows;
  private final OptionSpec<String> tags;

  public ShellOptions()
    {
    username = parser.accepts( "username", "name of remote user" )
      .withRequiredArg();

    password = parser.accepts( "password", "password of remote user" )
      .withRequiredArg();

    schema = parser.accepts( "schema", "name of schema to make default" )
      .withRequiredArg();

    schemas = parser.accepts( "schemas", "platform path for each schema to add" )
      .withRequiredArg().withValuesSeparatedBy( ',' );

    resultSchema = parser.accepts( "resultSchema", "the schema to store SELECT query results into" )
      .withOptionalArg().defaultsTo( "results" ).describedAs( "schema name" );

    resultPath = parser.accepts( "resultPath", "platform path to store results of SELECT queries" )
      .withOptionalArg().defaultsTo( "results" ).describedAs( "directory" );

    flowPlanPath = parser.accepts( "flowPlanPath", "platform path to write flow plan files" )
      .withOptionalArg().defaultsTo( "flowPlanFiles" ).describedAs( "directory" );

    sqlPlanPath = parser.accepts( "sqlPlanPath", "platform path to write SQL plan files" )
      .withOptionalArg().defaultsTo( "sqlPlanFiles" ).describedAs( "directory" );

    sqlFile = parser.accepts( "sql", "file with sql commands to execute, '-' for stdin" )
      .withRequiredArg().describedAs( "filename" );

    maxRows = parser.accepts( "maxRows", "number of results to limit. 0 for unlimited" )
      .withRequiredArg().ofType( Integer.TYPE ).defaultsTo( 10000 ).describedAs( "number of records" );

    tags = parser.accepts( "tags", "tags to annotate running processes with" )
      .withRequiredArg().withValuesSeparatedBy( ',' );
    }

  public String createJDBCUrl( Properties properties )
    {
    StringBuilder builder = new StringBuilder( "jdbc:lingual:" );

    builder.append( getPlatform() );

    if( getSchema() != null )
      builder.append( ":" ).append( getSchema() );

    addProperty( builder, Driver.CATALOG_PROP, properties, null ); // currently for testing

    addProperty( builder, Driver.SCHEMAS_PROP, properties, Util.join( getSchemas(), "," ) );

    addProperty( builder, Driver.RESULT_SCHEMA_PROP, properties, getResultSchema() );

    addProperty( builder, Driver.RESULT_PATH_PROP, properties, getResultPath() );

    addProperty( builder, Driver.MAX_ROWS, properties, stringOrNull( getMaxRows() ) );

    if( hasFlowPlanPath() )
      addProperty( builder, Driver.FLOW_PLAN_PATH, properties, getFlowPlanPath() );

    if( hasSQLPlanPath() )
      addProperty( builder, Driver.SQL_PLAN_PATH_PROP, properties, getSQLPlanPath() );

    if( hasTags() )
      addProperty( builder, Driver.TAGS_PROP, properties, Util.join( getTags(), "," ) );

    if( getSqlFile() != null )
      builder.append( ";" ).append( Driver.COLLECTOR_CACHE_PROP ).append( "=true" );

    if( hasConfig() )
      builder.append( ";" ).append( getConfigString() );

    // pass through from tests
    if( getProperty( "urlProperties", properties, null ) != null )
      builder.append( ";" ).append( getProperty( "urlProperties", properties, null ) );

    return builder.toString();
    }

  private void addProperty( StringBuilder builder, String property, Properties properties, String value )
    {
    String actual = getProperty( property, properties, value );

    if( actual == null )
      return;

    builder
      .append( ";" ).append( property ).append( "=" )
      .append( actual );
    }

  private String getProperty( String property, Properties properties, String value )
    {
    if( value != null && !value.isEmpty() )
      return value;

    return properties.getProperty( property );
    }

  private String stringOrNull( Object value )
    {
    if( value == null )
      return null;

    return value.toString();
    }

  /////

  public boolean hasUsername()
    {
    return optionSet.has( username );
    }

  public String getUsername()
    {
    return optionSet.valueOf( username );
    }

  public boolean hasPassword()
    {
    return optionSet.has( password );
    }

  public String getPassword()
    {
    return optionSet.valueOf( password );
    }

  public String getSchema()
    {
    return optionSet.valueOf( schema );
    }

  public List<String> getSchemas()
    {
    return optionSet.valuesOf( schemas );
    }

  public String getResultSchema()
    {
    return optionSet.valueOf( resultSchema );
    }

  public String getResultPath()
    {
    return optionSet.valueOf( resultPath );
    }

  public boolean hasFlowPlanPath()
    {
    return optionSet.has( flowPlanPath );
    }

  public String getFlowPlanPath()
    {
    return optionSet.valueOf( flowPlanPath );
    }

  public boolean hasSQLPlanPath()
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

  public Integer getMaxRows()
    {
    if( optionSet.valueOf( maxRows ) == null || optionSet.valueOf( maxRows ) == 0 )
      return null;

    return optionSet.valueOf( maxRows );
    }

  public boolean hasTags()
    {
    return optionSet.has( tags );
    }

  public List<String> getTags()
    {
    return optionSet.valuesOf( tags );
    }

  /////

  }
