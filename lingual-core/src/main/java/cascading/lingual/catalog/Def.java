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

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Class Def is the base class for all schema and table related meta-data. */
public class Def implements Serializable
  {
  @JsonBackReference
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

  protected void setParentSchema( SchemaDef parentSchema )
    {
    this.parentSchema = parentSchema;
    }

  public String getName()
    {
    return name;
    }

  protected void setName( String name )
    {
    this.name = name;
    }

  public String getIdentifier()
    {
    return identifier;
    }

  public void setIdentifier( String identifier )
    {
    this.identifier = identifier;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( getClass().getSimpleName() );
    sb.append( "{name='" ).append( name ).append( '\'' );
    sb.append( '}' );
    return sb.toString();
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof Def ) )
      return false;

    Def def = (Def) object;

    if( identifier != null ? !identifier.equals( def.identifier ) : def.identifier != null )
      return false;
    if( name != null ? !name.equals( def.name ) : def.name != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + ( identifier != null ? identifier.hashCode() : 0 );
    return result;
    }
  }
