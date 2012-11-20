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

/**
 *
 */
public class SchemaDef extends Def
  {
  private final Catalog<Protocol, Format> stereotypes = new Catalog<Protocol, Format>();

  private final Map<String, SchemaDef> schemas = new HashMap<String, SchemaDef>();
  private final Map<String, TableDef> tables = new HashMap<String, TableDef>();

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

  public boolean isRoot()
    {
    return getParentSchema() == null;
    }

  public Collection<SchemaDef> getChildSchemaDefs()
    {
    return schemas.values();
    }

  public Collection<TableDef> getChildTableDefs()
    {
    return tables.values();
    }

  public void addSchema( String name )
    {
    schemas.put( name, new SchemaDef( this, name ) );
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
  }
