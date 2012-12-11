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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.bind.catalog.Catalog;
import cascading.bind.catalog.Stereotype;
import cascading.lingual.util.MultiProperties;
import cascading.tuple.Fields;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaDef extends Def
  {
  @JsonIgnore
  private Catalog<Protocol, Format> stereotypes = new Catalog<Protocol, Format>();

  @JsonProperty
  private Map<String, SchemaDef> schemas = new HashMap<String, SchemaDef>();

  @JsonProperty
  private Map<String, TableDef> tables = new HashMap<String, TableDef>();

  @JsonProperty
  private MultiProperties<Protocol> protocolProperties = new MultiProperties<Protocol>();

  @JsonProperty
  private MultiProperties<Format> formatProperties = new MultiProperties<Format>();

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
    return schemas.values();
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
    return tables.values();
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

  public void addSchema( String name )
    {
    schemas.put( name, new SchemaDef( this, name ) );
    }

  public boolean removeSchema( String schemaName )
    {
    return schemas.remove( schemaName ) != null;
    }

  public boolean renameSchema( String schemaName, String newName )
    {
    SchemaDef schemaDef = schemas.remove( schemaName );

    if( schemaDef == null )
      return false;

    schemas.put( newName, schemaDef.copyWith( newName ) );

    return true;
    }

  public SchemaDef getOrAddSchema( String name )
    {
    SchemaDef schema = getSchema( name );

    if( schema == null )
      {
      schema = new SchemaDef( this, name );
      schemas.put( name, schema );
      }

    return schema;
    }

  public SchemaDef getSchema( String name )
    {
    return schemas.get( name );
    }

  public TableDef getTable( String name )
    {
    return tables.get( name );
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
    return tables.remove( tableName ) != null;
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
    TableDef tableDef = tables.remove( tableName );

    if( tableDef == null )
      return false;

    tables.put( newName, tableDef.copyWith( newName ) );

    return true;
    }

  public void addTable( String name, String identifier, Stereotype<Protocol, Format> stereotype, Protocol protocol, Format format )
    {
    if( tables.containsKey( name ) )
      throw new IllegalArgumentException( "table named: " + name + " already exists" );

    tables.put( name, new TableDef( this, name, identifier, stereotype, protocol, format ) );
    }

  public TableDef findTableFor( String identifier )
    {
    for( TableDef tableDef : tables.values() )
      {
      if( tableDef.getIdentifier().equals( identifier ) )
        return tableDef;
      }

    for( SchemaDef schemaDef : schemas.values() )
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
  }
