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

/** Class TableDef manages all "table" related meta-data. */
@JsonAutoDetect(
  fieldVisibility = JsonAutoDetect.Visibility.ANY,
  getterVisibility = JsonAutoDetect.Visibility.NONE,
  setterVisibility = JsonAutoDetect.Visibility.NONE
)
public class TableDef extends Def
  {
  @JsonProperty
  private String stereotypeName;
  @JsonProperty
  private Protocol protocol;
  @JsonProperty
  private Format format;

  protected TableDef()
    {
    }

  public TableDef( SchemaDef parentSchema, String name, String identifier )
    {
    super( parentSchema, name, identifier );
    }

  public TableDef( SchemaDef parentSchema, String name, String identifier, Stereotype<Protocol, Format> stereotype )
    {
    super( parentSchema, name, identifier );
    this.stereotypeName = stereotype.getName();
    this.protocol = stereotype.getDefaultProtocol();
    this.format = stereotype.getDefaultFormat();
    }

  public TableDef( SchemaDef parentSchema, String name, String identifier, Stereotype<Protocol, Format> stereotype, Protocol protocol, Format format )
    {
    super( parentSchema, name, identifier );
    this.stereotypeName = stereotype.getName();
    this.protocol = protocol == null ? stereotype.getDefaultProtocol() : protocol;
    this.format = format == null ? stereotype.getDefaultFormat() : format;
    }

  public TableDef( SchemaDef parentSchema, String name, String identifier, String stereotypeName, Protocol protocol, Format format )
    {
    super( parentSchema, name, identifier );
    this.stereotypeName = stereotypeName;
    this.protocol = protocol == null ? getStereotype().getDefaultProtocol() : protocol;
    this.format = format == null ? getStereotype().getDefaultFormat() : format;
    }

  public TableDef copyWith( String newName )
    {
    return new TableDef( parentSchema, newName, identifier, stereotypeName, protocol, format );
    }

  public Stereotype<Protocol, Format> getStereotype()
    {
    return getParentSchema().findStereotypeFor( stereotypeName );
    }

  public Fields getFields()
    {
    return getStereotype().getFields();
    }

  public void setStereotypeName( String stereotypeName )
    {
    this.stereotypeName = stereotypeName;
    }

  public String getStereotypeName()
    {
    return stereotypeName;
    }

  public void setProtocol( Protocol protocol )
    {
    this.protocol = protocol;
    }

  public Protocol getProtocol()
    {
    return protocol;
    }

  public void setFormat( Format format )
    {
    this.format = format;
    }

  public Format getFormat()
    {
    return format;
    }

  public Protocol getActualProtocol()
    {
    if( protocol != null )
      return protocol;

    return getParentSchema().findDefaultProtocol();
    }

  public Format getActualFormat()
    {
    if( format != null )
      return format;

    return getParentSchema().findDefaultFormat();
    }

  public Resource<Protocol, Format, SinkMode> getResourceWith( SinkMode sinkMode )
    {
    return new Resource<Protocol, Format, SinkMode>( getParentSchema().getName(), identifier, getActualProtocol(), getActualFormat(), sinkMode );
    }

  public ProviderDef getProtocolProvider()
    {
    return parentSchema.getProtocolProvider( getActualProtocol() );
    }

  public ProviderDef getFormatProvider()
    {
    return parentSchema.getFormatProvider( getActualFormat() );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof TableDef ) )
      return false;
    if( !super.equals( object ) )
      return false;

    TableDef tableDef = (TableDef) object;

    if( format != null ? !format.equals( tableDef.format ) : tableDef.format != null )
      return false;
    if( protocol != null ? !protocol.equals( tableDef.protocol ) : tableDef.protocol != null )
      return false;
    if( stereotypeName != null ? !stereotypeName.equals( tableDef.stereotypeName ) : tableDef.stereotypeName != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( stereotypeName != null ? stereotypeName.hashCode() : 0 );
    result = 31 * result + ( protocol != null ? protocol.hashCode() : 0 );
    result = 31 * result + ( format != null ? format.hashCode() : 0 );
    return result;
    }
  }
