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
import java.util.Map;

import cascading.bind.catalog.Catalog;
import cascading.bind.catalog.Stereotype;
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

  public Collection<SchemaDef> getChildSchemaDefs()
    {
    return schemas.values();
    }

  public Collection<String> getChildSchemaNames()
    {
    return Collections2.transform( getChildSchemaDefs(), new Function<SchemaDef, String>()
    {
    @Override
    public String apply( SchemaDef input )
      {
      return input.getName();
      }
    } );
    }

  public Collection<TableDef> getChildTableDefs()
    {
    return tables.values();
    }

  public Collection<String> getChildTableNames()
    {
    return Collections2.transform( getChildTableDefs(), new Function<TableDef, String>()
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

  public void addTable( String name, String identifier, Stereotype<Protocol, Format> stereotype )
    {
    if( tables.containsKey( name ) )
      throw new IllegalArgumentException( "table named: " + name + " already exists" );

    tables.put( name, new TableDef( this, name, identifier, stereotype ) );
    }

  public void addStereotype( Stereotype<Protocol, Format> stereotype )
    {
    stereotypes.addStereotype( stereotype );
    }

  public Stereotype<Protocol, Format> getStereotypeFor( Fields fields )
    {
    return stereotypes.getStereotypeFor( fields );
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
  }
