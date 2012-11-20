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

import joptsimple.OptionSpec;

/**
 *
 */
public class CatalogOptions extends cascading.lingual.common.Options
  {
  private final OptionSpec<String> uri;
  private final OptionSpec<String> schema;
  private final OptionSpec<String> table;

  private final OptionSpec<String> add;
  private final OptionSpec<Void> remove;
  private final OptionSpec<String> rename;

  public CatalogOptions()
    {
    super();

    uri = parser.accepts( "uri", "path to catalog location, defaults is current directory" )
      .withRequiredArg().defaultsTo( "./" );

    schema = parser.accepts( "schema", "name of schema to use" )
      .withOptionalArg();

    table = parser.accepts( "table", "name of table to use" )
      .withOptionalArg();

    add = parser.accepts( "add", "uri path to schema or table" )
      .withRequiredArg();

    remove = parser.accepts( "remove", "remove the specified schema or table" );

    rename = parser.accepts( "rename", "rename the specified schema or table to given name" )
      .withRequiredArg();

    }

  @Override
  protected void validate()
    {
    super.validate();


    }

  /////

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

  public boolean isListTables()
    {
    return isSetWithNoArg( table );
    }

  public String getTableName()
    {
    return optionSet.valueOf( table );
    }

  /////

  public String getAddURI()
    {
    return optionSet.valueOf( add );
    }

  /////

  }
