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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import cascading.lingual.common.Options;
import cascading.lingual.common.PropertiesConverter;
import cascading.lingual.common.PropertiesFileConverter;
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
  private final OptionSpec<String> provider;
  private final OptionSpec<String> repo;

  private final OptionSpec<String> add;
  private final OptionSpec<String> update;
  private final OptionSpec<Void> remove;
  private final OptionSpec<String> rename;
  private final OptionSpec<Void> show;

  private final OptionSpec<Map<String, String>> properties;
  private final OptionSpec<Map<String, String>> propertiesFromFile;

  private final OptionSpec<String> extensions;
  private final OptionSpec<String> schemes;

  private final OptionSpec<String> columns;
  private final OptionSpec<String> types;

  private final OptionSpec<String> validate;

  public CatalogOptions()
    {
    init = parser.accepts( "init", "initializes meta-data store" );

    uri = parser.accepts( "uri", "path to catalog location, defaults is current directory on current platform" )
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

    provider = parser.accepts( "provider", "provider definition" )
      .withOptionalArg().describedAs( "name of provider to use from specified jar" );

    repo = parser.accepts( "repo", "Maven repo management" )
      .withOptionalArg();

    add = parser.accepts( "add", "uri path to schema, table, or provider. or maven spec 'group:name:rev[:classifier]'" )
      .withOptionalArg();

    update = parser.accepts( "update", "uri path to schema, table, or provider. or maven spec 'group:name:rev[:classifier]'" )
      .withOptionalArg();

    remove = parser.accepts( "remove", "remove the named schema, table, etc" );

    rename = parser.accepts( "rename", "remove the named schema, table, etc to the given name" )
      .withRequiredArg();

    show = parser.accepts( "show", "shows properties assigned to a schema, table, stereotype, format, or provider" );

    properties = parser.acceptsAll( asList( "props", "properties" ), "key=value pairs" )
      .withRequiredArg().withValuesConvertedBy( new PropertiesConverter() );
    
    propertiesFromFile = parser.accepts( "properties-file", "filename" )
        .withRequiredArg().withValuesConvertedBy( new PropertiesFileConverter() );

    extensions = parser.acceptsAll( asList( "exts", "extensions" ), "file name extension to associate with format, .csv, .tsv, ..." )
      .withRequiredArg().withValuesSeparatedBy( ',' );

    schemes = parser.accepts( "schemes", "uri schemes to associate with protocol, http:, jdbc:, ..." )
      .withRequiredArg().withValuesSeparatedBy( ',' );

    columns = parser.accepts( "columns", "columns names of the stereotype" )
      .withRequiredArg().withValuesSeparatedBy( ',' );

    types = parser.accepts( "types", "types for each column" )
      .withRequiredArg().withValuesSeparatedBy( ',' );

    validate = parser.accepts( "validate", "confirms that a maven repo or provider is valid without adding it" )
      .withOptionalArg();
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
    return optionSet.has( add ) || optionSet.has( update ) || optionSet.has( remove ) || optionSet.has( rename ) || optionSet.has( validate ) || optionSet.has( show );
    }

  public String getURI()
    {
    return optionSet.valueOf( uri );
    }

  public boolean isList()
    {
    return isListSchemas() || isListFormats() || isListTables() || isListStereotypes() || isListFormats()
      || isListProtocols() || isListProviders() || isListRepos();
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
      && !isProviderActions()
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

  public boolean hasProperties()
    {
    return optionSet.has( properties ) || optionSet.has( propertiesFromFile );
    }

  public Map<String, String> getProperties()
    {
    List<Map<String, String>> allMaps = Lists.newArrayList();
    List<Map<String, String>> propertiesMaps = optionSet.valuesOf( properties );
    if (propertiesMaps != null)
      allMaps.addAll( propertiesMaps );
    List<Map<String, String>> propertiesFromFileMaps = optionSet.valuesOf( propertiesFromFile );
    if (propertiesFromFileMaps != null)
      allMaps.addAll( propertiesFromFileMaps );

    Map<String, String> results = new LinkedHashMap<String, String>();

    for( Map<String, String> map : allMaps )
      results.putAll( map );

    return results;
    }

  public List<String> getExtensions()
    {
    return optionSet.valuesOf( extensions );
    }

  public List<String> getSchemes()
    {
    return optionSet.valuesOf( schemes );
    }

  public List<String> getColumns()
    {
    return optionSet.valuesOf( columns );
    }

  public List<String> getTypes()
    {
    return optionSet.valuesOf( types );
    }

  public boolean isListProviders()
    {
    return isSetWithNoArg( provider ) && !isActions();
    }

  public boolean isProviderActions()
    {
    return optionSet.has( provider ) && isActions() && !isTableActions();
    }

  public String getProviderName()
    {
    return optionSet.valueOf( provider );
    }

  public boolean isListRepos()
    {
    return isSetWithNoArg( repo );
    }

  public boolean isRepoActions()
    {
    return optionSet.hasArgument( repo ) && isActions() && !isTableActions();
    }

  public String getRepoName()
    {
    return optionSet.valueOf( repo );
    }

  public boolean isShow()
    {
    return optionSet.has( show );
    }

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

  public boolean isValidate()
    {
    return optionSet.has( validate );
    }
  }
