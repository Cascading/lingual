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

import cascading.bind.catalog.Resource;
import cascading.bind.catalog.Stereotype;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
@JsonAutoDetect(
  fieldVisibility = JsonAutoDetect.Visibility.ANY,
  getterVisibility = JsonAutoDetect.Visibility.NONE,
  setterVisibility = JsonAutoDetect.Visibility.NONE
)
public class TableDef extends Def
  {
  @JsonProperty
  private Stereotype<Protocol, Format> stereotype;
  @JsonProperty
  private Protocol protocol;
  @JsonProperty
  private Format format;

  protected TableDef()
    {
    }

  public TableDef( SchemaDef parentSchema, String name, String identifier, Stereotype<Protocol, Format> stereotype )
    {
    super( parentSchema, name, identifier );
    this.stereotype = stereotype;
    }

  public TableDef( SchemaDef parentSchema, String name, String identifier, Stereotype<Protocol, Format> stereotype, Protocol protocol, Format format )
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

  public Stereotype<Protocol, Format> getStereotype()
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

  public Resource<Protocol, Format, SinkMode> getResourceWith( SinkMode sinkMode )
    {
    return new Resource<Protocol, Format, SinkMode>( identifier, protocol, format, sinkMode );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      {
      return true;
      }
    if( !( object instanceof TableDef ) )
      {
      return false;
      }
    if( !super.equals( object ) )
      {
      return false;
      }

    TableDef tableDef = (TableDef) object;

    if( format != null ? !format.equals( tableDef.format ) : tableDef.format != null )
      {
      return false;
      }
    if( protocol != null ? !protocol.equals( tableDef.protocol ) : tableDef.protocol != null )
      {
      return false;
      }
    if( stereotype != null ? !stereotype.equals( tableDef.stereotype ) : tableDef.stereotype != null )
      {
      return false;
      }

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( stereotype != null ? stereotype.hashCode() : 0 );
    result = 31 * result + ( protocol != null ? protocol.hashCode() : 0 );
    result = 31 * result + ( format != null ? format.hashCode() : 0 );
    return result;
    }
  }
