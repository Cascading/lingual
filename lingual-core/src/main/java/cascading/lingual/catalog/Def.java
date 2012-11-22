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

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class Def implements Serializable
  {
  @JsonIgnore
  protected SchemaDef parentSchema;
  @JsonProperty
  protected String name;
  @JsonProperty
  protected String identifier;

  public Def()
    {
    }

  public Def( SchemaDef parentSchema, String name )
    {
    this.parentSchema = parentSchema;
    this.name = name;
    }

  public Def( SchemaDef parentSchema, String name, String identifier )
    {
    this.parentSchema = parentSchema;
    this.name = name;
    this.identifier = identifier;
    }

  public SchemaDef getParentSchema()
    {
    return parentSchema;
    }

  public String getName()
    {
    return name;
    }

  public String getIdentifier()
    {
    return identifier;
    }
  }
