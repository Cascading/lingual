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

import java.util.List;

import cascading.lingual.common.Options;
import joptsimple.OptionSpec;

import static java.util.Arrays.asList;

/**
 *
 */
public class CatalogOptions extends Options
  {
  private final OptionSpec<Void> init;

  private final OptionSpec<String> ddl;

  private final OptionSpec<String> uri;
  private final OptionSpec<String> schema;
  private final OptionSpec<String> table;
  private final OptionSpec<String> stereotype;
  private final OptionSpec<String> format;
  private final OptionSpec<String> protocol;

  private final OptionSpec<String> add;
  private final OptionSpec<String> update;
  private final OptionSpec<Void> remove;
  private final OptionSpec<String> rename;

  private final OptionSpec<String> extensions;
  private final OptionSpec<String> uris;

  private final OptionSpec<String> columns;
  private final OptionSpec<String> types;

  public CatalogOptions()
    {
    init = parser.accepts( "init", "initializes meta-data store" );

    uri = parser.accepts( "uri", "path to catalog location, defaults is current directory" )
      .withRequiredArg().describedAs( "directory" ).defaultsTo( "./" );

    ddl = parser.accepts( "ddl", "initializes schema with DDL commands" )
      .withRequiredArg().describedAs( "filename" );

    schema = parser.accepts( "schema", "name of schema to use" )
      .withOptionalArg();

    table = parser.accepts( "table", "name of table to use" )
      .withOptionalArg();

    stereotype = parser.accepts( "stereotype", "name of stereotype to use" )
      .withOptionalArg();

    format = parser.accepts( "format", "name of format to use" )
      .withOptionalArg();

    protocol = parser.accepts( "protocol", "name of protocol to use" )
      .withOptionalArg();

    add = parser.accepts( "add", "possible uri path to schema or table" )
      .withOptionalArg();

    update = parser.accepts( "update", "possible uri path to schema or table" )
      .withOptionalArg();

    remove = parser.accepts( "remove", "remove the specified schema or table" );

    rename = parser.accepts( "rename", "rename the specified schema or table to given name" )
      .withRequiredArg();

    extensions = parser.acceptsAll( asList( "exts", "extensions" ), "file name extension to associate with format, .csv, .tsv, ..." )
      .withRequiredArg().withValuesSeparatedBy( ',' );

    uris = parser.accepts( "uris", "uri schemes to associate with protocol, http:, jdbc:, ..." )
      .withRequiredArg().withValuesSeparatedBy( ',' );

    columns = parser.accepts( "columns", "columns names of the stereotype" )
      .withRequiredArg().withValuesSeparatedBy( ',' );

    types = parser.accepts( "types", "types for each column" )
      .withRequiredArg().withValuesSeparatedBy( ',' );

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

  public boolean isDDL()
    {
    return optionSet.has( ddl );
    }

  public String getDDL()
    {
    return optionSet.valueOf( ddl );
    }

  public boolean isActions()
    {
    return optionSet.has( add ) || optionSet.has( update ) || optionSet.has( remove ) || optionSet.has( rename );
    }

  public String getURI()
    {
    return optionSet.valueOf( uri );
    }

  public boolean isList()
    {
    return isListSchemas() || isListFormats() || isListTables() || isListStereotypes() || isListFormats() || isListProtocols();
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
    return optionSet.hasArgument( schema ) && isActions()
      && !isTableActions()
      && !isStereotypeActions()
      && !isProtocolActions()
      && !isFormatActions();
    }

  public boolean isTableActions()
    {
    return optionSet.hasArgument( table ) && isActions();
    }

  public boolean isListTables()
    {
    return isSetWithNoArg( table );
    }

  public String getTableName()
    {
    return optionSet.valueOf( table );
    }

  public boolean isListStereotypes()
    {
    return isSetWithNoArg( stereotype );
    }

  public boolean isStereotypeActions()
    {
    return optionSet.hasArgument( stereotype ) && isActions() && !isTableActions();
    }

  public String getStereotypeName()
    {
    return optionSet.valueOf( stereotype );
    }

  public boolean isListFormats()
    {
    return isSetWithNoArg( format );
    }

  public boolean isFormatActions()
    {
    return optionSet.hasArgument( format ) && isActions() && !isTableActions();
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
    return optionSet.hasArgument( protocol ) && isActions() && !isTableActions();
    }

  public List<String> getExtensions()
    {
    return optionSet.valuesOf( extensions );
    }

  public List<String> getURIs()
    {
    return optionSet.valuesOf( uris );
    }

  public List<String> getColumns()
    {
    return optionSet.valuesOf( columns );
    }

  public List<String> getTypes()
    {
    return optionSet.valuesOf( types );
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

  public boolean isUpdate()
    {
    return optionSet.has( update );
    }

  public String getUpdateURI()
    {
    return optionSet.valueOf( update );
    }

  public String getAddOrUpdateURI()
    {
    if( isAdd() )
      return getAddURI();
    else if( isUpdate() )
      return getUpdateURI();

    throw new IllegalStateException( "not update or add" );
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
