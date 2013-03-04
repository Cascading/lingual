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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.bind.catalog.Stereotype;
import cascading.bind.catalog.Stereotypes;
import cascading.lingual.util.InsensitiveMap;
import cascading.lingual.util.MultiProperties;
import cascading.tuple.Fields;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 *
 */
@JsonAutoDetect(
  fieldVisibility = JsonAutoDetect.Visibility.ANY,
  getterVisibility = JsonAutoDetect.Visibility.NONE,
  setterVisibility = JsonAutoDetect.Visibility.NONE,
  isGetterVisibility = JsonAutoDetect.Visibility.NONE
)
public class SchemaDef extends Def
  {
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private final Stereotypes<Protocol, Format> stereotypes = new Stereotypes<Protocol, Format>();

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private final InsensitiveMap<SchemaDef> childSchemas = new InsensitiveMap<SchemaDef>();

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private final InsensitiveMap<TableDef> childTables = new InsensitiveMap<TableDef>();

  @JsonProperty
  private final MultiProperties<Protocol> protocolProperties = new MultiProperties<Protocol>();

  @JsonProperty
  private final MultiProperties<Format> formatProperties = new MultiProperties<Format>();

  public SchemaDef()
    {
    }

  public SchemaDef( SchemaDef parentSchema, String name )
    {
    super( parentSchema, name );
    }

  public SchemaDef( SchemaDef parentSchema, String name, String identifier )
    {
    super( parentSchema, name, identifier );
    }

  public SchemaDef copyWith( String name )
    {
    return new SchemaDef( parentSchema, name, identifier );
    }

  public boolean isRoot()
    {
    return getParentSchema() == null;
    }

  public void addProtocolProperties( Protocol protocol, Map<String, List<String>> properties )
    {
    protocolProperties.addProperties( protocol, properties );
    }

  public void addProtocolProperty( Protocol protocol, String property, String... values )
    {
    protocolProperties.addProperty( protocol, property, values );
    }

  public void addProtocolProperty( Protocol protocol, String property, List<String> values )
    {
    protocolProperties.addProperty( protocol, property, values );
    }

  public Map<Protocol, List<String>> getProtocolProperties( String property )
    {
    Map<Protocol, List<String>> values = new HashMap<Protocol, List<String>>();

    populateProtocolProperty( values, property );

    return values;
    }

  private void populateProtocolProperty( Map<Protocol, List<String>> values, String property )
    {
    if( !isRoot() )
      getParentSchema().populateProtocolProperty( values, property );

    values.putAll( protocolProperties.getKeyFor( property ) );
    }

  public void addFormatProperties( Format format, Map<String, List<String>> properties )
    {
    formatProperties.addProperties( format, properties );
    }

  public void addFormatProperty( Format format, String property, String... values )
    {
    formatProperties.addProperty( format, property, values );
    }

  public void addFormatProperty( Format format, String property, List<String> values )
    {
    formatProperties.addProperty( format, property, values );
    }

  public Map<Format, List<String>> getFormatProperties( String property )
    {
    Map<Format, List<String>> values = new HashMap<Format, List<String>>();

    populateFormatProperty( values, property );

    return values;
    }

  private void populateFormatProperty( Map<Format, List<String>> values, String property )
    {
    if( !isRoot() )
      getParentSchema().populateFormatProperty( values, property );

    values.putAll( formatProperties.getKeyFor( property ) );
    }

  public Collection<SchemaDef> getChildSchemas()
    {
    return childSchemas.values();
    }

  public Collection<String> getChildSchemaNames()
    {
    return Collections2.transform( getChildSchemas(), new Function<SchemaDef, String>()
    {
    @Override
    public String apply( SchemaDef input )
      {
      return input.getName();
      }
    } );
    }

  public Collection<TableDef> getChildTables()
    {
    return childTables.values();
    }

  public Collection<String> getChildTableNames()
    {
    return Collections2.transform( getChildTables(), new Function<TableDef, String>()
    {
    @Override
    public String apply( TableDef input )
      {
      return input.getName();
      }
    } );
    }

  public boolean addSchema( String name )
    {
    if( childSchemas.containsKey( name ) )
      return false;

    childSchemas.put( name, new SchemaDef( this, name ) );

    return true;
    }

  public boolean addSchema( String name, String identifier )
    {
    if( childSchemas.containsKey( name ) )
      return false;

    childSchemas.put( name, new SchemaDef( this, name, identifier ) );

    return true;
    }

  public boolean removeSchema( String schemaName )
    {
    return childSchemas.remove( schemaName ) != null;
    }

  public boolean renameSchema( String schemaName, String newName )
    {
    SchemaDef schemaDef = childSchemas.remove( schemaName );

    if( schemaDef == null )
      return false;

    childSchemas.put( newName, schemaDef.copyWith( newName ) );

    return true;
    }

  public SchemaDef getOrAddSchema( String name )
    {
    SchemaDef schema = getSchema( name );

    if( schema == null )
      addSchema( name );

    return getSchema( name );
    }

  public SchemaDef getSchema( String name )
    {
    return childSchemas.get( name );
    }

  public TableDef getTable( String name )
    {
    return childTables.get( name );
    }

  public boolean removeTable( String schemaName, String tableName )
    {
    SchemaDef schemaDef = getSchema( schemaName );

    if( schemaDef == null )
      return false;

    return schemaDef.removeTable( tableName );
    }

  private boolean removeTable( String tableName )
    {
    return childTables.remove( tableName ) != null;
    }

  public boolean renameTable( String schemaName, String tableName, String newName )
    {
    SchemaDef schemaDef = getSchema( schemaName );

    if( schemaDef == null )
      return false;

    return schemaDef.renameTable( tableName, newName );
    }

  private boolean renameTable( String tableName, String newName )
    {
    TableDef tableDef = childTables.remove( tableName );

    if( tableDef == null )
      return false;

    childTables.put( newName, tableDef.copyWith( newName ) );

    return true;
    }

  public void addTable( String name, String identifier, Stereotype<Protocol, Format> stereotype, Protocol protocol, Format format )
    {
    if( childTables.containsKey( name ) )
      throw new IllegalArgumentException( "table named: " + name + " already exists" );

    childTables.put( name, new TableDef( this, name, identifier, stereotype, protocol, format ) );
    }

  public TableDef findTableFor( String identifier )
    {
    for( TableDef tableDef : childTables.values() )
      {
      if( tableDef.getIdentifier().equalsIgnoreCase( identifier ) )
        return tableDef;
      }

    for( SchemaDef schemaDef : childSchemas.values() )
      {
      TableDef tableDef = schemaDef.findTableFor( identifier );

      if( tableDef != null )
        return tableDef;
      }

    return null;
    }

  public Stereotype<Protocol, Format> getStereotype( String stereotypeName )
    {
    if( stereotypeName == null || stereotypeName.isEmpty() )
      return null;

    return stereotypes.getStereotypeFor( stereotypeName );
    }

  public void addStereotype( Stereotype<Protocol, Format> stereotype )
    {
    stereotypes.addStereotype( stereotype );
    }

  public boolean removeStereotype( String name )
    {
    return stereotypes.removeStereotype( name );
    }

  public boolean renameStereotype( String name, String newName )
    {
    return stereotypes.renameStereotype( name, newName );
    }

  public Stereotype<Protocol, Format> getStereotypeFor( Fields fields )
    {
    Stereotype<Protocol, Format> stereotypeFor = stereotypes.getStereotypeFor( fields );

    if( stereotypeFor != null || getParentSchema() == null )
      return stereotypeFor;

    return getParentSchema().getStereotypeFor( fields );
    }

  public boolean hasStereotype( String name )
    {
    if( name == null )
      return false;

    return stereotypes.getStereotypeFor( name ) != null;
    }

  public Collection<String> getFormatNames()
    {
    return Collections2.transform( stereotypes.getAllFormats(), new Function<Format, String>()
    {
    @Override
    public String apply( Format input )
      {
      return input.toString();
      }
    } );
    }

  public Collection<String> getProtocolNames()
    {
    return Collections2.transform( stereotypes.getAllProtocols(), new Function<Protocol, String>()
    {
    @Override
    public String apply( Protocol input )
      {
      return input.toString();
      }
    } );
    }

  public Collection<String> getStereotypeNames()
    {
    return stereotypes.getStereotypeNames();
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof SchemaDef ) )
      return false;
    if( !super.equals( object ) )
      return false;

    SchemaDef schemaDef = (SchemaDef) object;

    if( childSchemas != null ? !childSchemas.equals( schemaDef.childSchemas ) : schemaDef.childSchemas != null )
      return false;
    if( childTables != null ? !childTables.equals( schemaDef.childTables ) : schemaDef.childTables != null )
      return false;
    if( formatProperties != null ? !formatProperties.equals( schemaDef.formatProperties ) : schemaDef.formatProperties != null )
      return false;
    if( protocolProperties != null ? !protocolProperties.equals( schemaDef.protocolProperties ) : schemaDef.protocolProperties != null )
      return false;
    if( stereotypes != null ? !stereotypes.equals( schemaDef.stereotypes ) : schemaDef.stereotypes != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( stereotypes != null ? stereotypes.hashCode() : 0 );
    result = 31 * result + ( childSchemas != null ? childSchemas.hashCode() : 0 );
    result = 31 * result + ( childTables != null ? childTables.hashCode() : 0 );
    result = 31 * result + ( protocolProperties != null ? protocolProperties.hashCode() : 0 );
    result = 31 * result + ( formatProperties != null ? formatProperties.hashCode() : 0 );
    return result;
    }
  }
