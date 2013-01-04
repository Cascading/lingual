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

import cascading.bind.catalog.Stereotype;
import cascading.tuple.Fields;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class TableDef extends Def
  {
  @JsonProperty
  private Stereotype stereotype;

  private Protocol protocol;

  private Format format;

  public TableDef( SchemaDef parentSchema, String name, String identifier, Stereotype stereotype )
    {
    super( parentSchema, name, identifier );
    this.stereotype = stereotype;
    }

  public TableDef( SchemaDef parentSchema, String name, String identifier, Stereotype stereotype, Protocol protocol, Format format )
    {
    super( parentSchema, name, identifier );
    this.stereotype = stereotype;
    this.protocol = protocol;
    this.format = format;
    }

  public TableDef copyWith( String newName )
    {
    return new TableDef( parentSchema, newName, identifier, stereotype );
    }

  public Stereotype getStereotype()
    {
    return stereotype;
    }

  public Fields getFields()
    {
    return stereotype.getFields();
    }

  public Protocol getProtocol()
    {
    return protocol;
    }

  public Format getFormat()
    {
    return format;
    }
  }
