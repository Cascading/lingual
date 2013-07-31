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

package cascading.lingual.tap;

import cascading.lingual.catalog.SchemaDef;
import cascading.lingual.catalog.TableDef;
import cascading.lingual.jdbc.LingualConnection;
import cascading.lingual.platform.PlatformBroker;
import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.impl.TableInSchemaImpl;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.impl.java.MapSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class TapSchema is an implementation of the Optiq {@link MapSchema} base class. */
public class TapSchema extends MapSchema
  {
  private static final Logger LOG = LoggerFactory.getLogger( TapSchema.class );

  private LingualConnection connection;
  private PlatformBroker platformBroker;
  private MapSchema parent;
  private String name;
  private String identifier;

  private static Expression makeExpression( String name, MutableSchema rootSchema )
    {
    return rootSchema.getSubSchemaExpression( name, Object.class );
    }

  public TapSchema( QueryProvider queryProvider, JavaTypeFactory typeFactory )
    {
    super( null, queryProvider, typeFactory, "root", Expressions.parameter( Object.class, "root" ) );
    }

  public TapSchema( TapSchema parent, String name )
    {
    super( parent, name, makeExpression( name, parent ) );
    this.parent = parent;
    this.name = name;
    }

  public TapSchema( MapSchema parent, LingualConnection connection, SchemaDef schemaDef )
    {
    this( parent, connection, schemaDef.getName(), schemaDef.getIdentifier() );
    }

  public TapSchema( MapSchema parent, LingualConnection connection, String name, String identifier )
    {
    super( parent, name, makeExpression( name, connection.getRootSchema() ) );
    this.parent = parent;
    this.connection = connection;
    this.platformBroker = connection.getPlatformBroker();
    this.name = name;
    this.identifier = identifier;
    }

  public String getFullName()
    {
    if( parent == null )
      return null;

    String parentName = parent instanceof TapSchema ? ( (TapSchema) parent ).getFullName() : null;

    if( parentName == null )
      return getName();

    return parentName + '.' + getName();
    }

  public String getName()
    {
    return name;
    }

  public String getIdentifier()
    {
    return identifier;
    }

  public void addTapTablesFor( SchemaDef schemaDef )
    {
    for( TableDef tableDef : schemaDef.getChildTables() )
      addTapTableFor( tableDef );
    }

  public TapTable addTapTableFor( TableDef tableDef )
    {
    return addTapTableFor( tableDef, false );
    }

  public TapTable addTapTableFor( TableDef tableDef, boolean useFullName )
    {
    TapTable found = (TapTable) getTable( tableDef.getName(), Object.class );

    if( found != null )
      LOG.info( "replacing table on schema: {}, table: {}, fields: {}, identifier: {}",
        new Object[]{getFullName(), found.getName(), found.getFields(), found.getIdentifier()} );

    TapTable table = new TapTable( platformBroker, getQueryProvider(), this, tableDef, useFullName );

    LOG.info( "adding table on schema: {}, table: {}, fields: {}, identifier: {}",
      new Object[]{getFullName(), table.getName(), table.getFields(), table.getIdentifier()} );

    addTable( table.getName(), table );

    return table;
    }

  public void addTable( String tableName, TapTable tapTable )
    {
    addTable( createTableInSchema( tableName, tapTable ) );
    }

  protected TableInSchemaImpl createTableInSchema( String tableName, TapTable table )
    {
    return new TableInSchemaImpl( this, tableName, TableType.TABLE, table );
    }
  }
