/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.lingual.common.Options;
import joptsimple.OptionSpec;

/**
 *
 */
public class CatalogOptions extends Options
  {
  private final OptionSpec<Void> init;

  private final OptionSpec<String> uri;
  private final OptionSpec<String> schema;
  private final OptionSpec<String> format;
  private final OptionSpec<String> protocol;

  private final OptionSpec<String> table;
  private final OptionSpec<String> add;
  private final OptionSpec<Void> remove;
  private final OptionSpec<String> rename;

  private OptionSpec<String> scheme;
  private OptionSpec<String> schemeProperties;
  private OptionSpec<String> extensions;

  public CatalogOptions()
    {
    super();

    init = parser.accepts( "init", "initializes meta-data store" );

    uri = parser.accepts( "uri", "path to catalog location, defaults is current directory" )
      .withRequiredArg().defaultsTo( "./" );

    schema = parser.accepts( "schema", "name of schema to use" )
      .withOptionalArg();

    table = parser.accepts( "table", "name of table to use" )
      .withOptionalArg();

    format = parser.accepts( "format", "name of format to use" )
      .withOptionalArg();

    protocol = parser.accepts( "protocol", "name of protocol to use" )
      .withOptionalArg();

    add = parser.accepts( "add", "uri path to schema or table" )
      .withRequiredArg();

    remove = parser.accepts( "remove", "remove the specified schema or table" );

    rename = parser.accepts( "rename", "rename the specified schema or table to given name" )
      .withRequiredArg();

    extensions = parser.accepts( "exts", "file name extension to associate with format" )
      .withRequiredArg().withValuesSeparatedBy( ',' );

    scheme = parser.accepts( "scheme", "name of format to use" )
      .withRequiredArg();

    schemeProperties = parser.accepts( "scheme-properties", "name of format to use" )
      .withRequiredArg();
    }

  @Override
  protected void validate()
    {
    super.validate();
    }

  /////

  public boolean isInit()
    {
    return optionSet.has( init );
    }

  public String getURI()
    {
    return optionSet.valueOf( uri );
    }

  public boolean isListSchemas()
    {
    return isSetWithNoArg( schema );
    }

  public String getSchemaName()
    {
    return optionSet.valueOf( schema );
    }

  public boolean isSchemaActions()
    {
    return getSchemaName() != null && !optionSet.has( table );
    }

  public boolean isTableActions()
    {
    return getSchemaName() != null && optionSet.has( table );
    }

  public boolean isListTables()
    {
    return !isListSchemas() && isSetWithNoArg( table );
    }

  public String getTableName()
    {
    return optionSet.valueOf( table );
    }

  public boolean isListFormats()
    {
    return isSetWithNoArg( format );
    }

  public boolean isFormatActions()
    {
    return getFormatName() != null && optionSet.has( format );
    }

  public String getFormatName()
    {
    return optionSet.valueOf( format );
    }

  public boolean isListProtocols()
    {
    return isSetWithNoArg( protocol );
    }

  public String getProtocolName()
    {
    return optionSet.valueOf( protocol );
    }

  public boolean isProtocolActions()
    {
    return getProtocolName() != null && optionSet.has( protocol );
    }

  public String getSchemeClassname()
    {
    return optionSet.valueOf( scheme );
    }

  public Map<String, String> getSchemeOptions()
    {
    Map<String, String> results = new HashMap<String, String>();
    List<String> options = optionSet.valuesOf( schemeProperties );
    for( String option : options )
      {
      String[] entry = option.split( "=" );

      results.put( entry[ 0 ], entry.length == 1 ? null : entry[ 1 ] );
      }

    return results;
    }

  /////

  public boolean isAdd()
    {
    return optionSet.has( add );
    }

  public String getAddURI()
    {
    return optionSet.valueOf( add );
    }

  public boolean isRemove()
    {
    return optionSet.has( remove );
    }

  public boolean isRename()
    {
    return optionSet.has( rename );
    }

  public String getRenameName()
    {
    return optionSet.valueOf( rename );
    }
  }
