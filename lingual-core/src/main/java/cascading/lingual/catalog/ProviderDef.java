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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.lingual.catalog.provider.ProviderDefinition;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(
  fieldVisibility = JsonAutoDetect.Visibility.ANY,
  getterVisibility = JsonAutoDetect.Visibility.NONE,
  setterVisibility = JsonAutoDetect.Visibility.NONE
)
public class ProviderDef extends Def
  {
  @JsonProperty
  protected Map<String, String> properties = new HashMap<String, String>(); // raw property set found in provider jar

  @JsonProperty
  protected String fileHash = null;

  // Jackson needs this.
  private ProviderDef()
    {
    }

  public ProviderDef( SchemaDef schemaDef, String name, String identifier, Map<String, String> properties )
    {
    super( schemaDef, name, identifier );

    if( properties != null )
      this.properties.putAll( properties );
    }

  public ProviderDef( SchemaDef schemaDef, String name, String identifier, Map<String, String> properties, String fileHash )
    {
    this( schemaDef, name, identifier, properties );
    this.fileHash = fileHash;
    }

  public String getName()
    {
    return name;
    }

  public Map<String, String> getProperties()
    {
    return properties;
    }

  public String getExtends()
    {
    return new ProviderDefinition( getName(), properties ).getExtends();
    }

  public String getFactoryClassName()
    {
    return new ProviderDefinition( getName(), properties ).getFactoryClassName();
    }

  public Map<Protocol, Map<String, List<String>>> getProtocolProperties()
    {
    return new ProviderDefinition( getName(), properties ).getDefaultProtocolProperties();
    }

  public Map<Format, Map<String, List<String>>> getFormatProperties()
    {
    return new ProviderDefinition( getName(), properties ).getDefaultFormatProperties();
    }

  public String getDescription()
    {
    return new ProviderDefinition( getName(), properties ).getDescription();
    }

  public String getFileHash()
    {
    return fileHash;
    }

//  public boolean handlesProtocols()
//    {
//    return !getProtocolProperties().keySet().isEmpty();
//    }

//  public boolean handlesFormats()
//    {
//    return !getFormatProperties().keySet().isEmpty();
//    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;

    if( !( object instanceof ProviderDef ) )
      return false;

    ProviderDef that = (ProviderDef) object;

    if( name != null ? !name.equals( that.name ) : that.name != null )
      return false;

    if( identifier != null ? !identifier.equals( that.identifier ) : that.identifier != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + ( identifier != null ? identifier.hashCode() : 0 );
    result = 31 * result + super.hashCode();
    return result;
    }
  }


