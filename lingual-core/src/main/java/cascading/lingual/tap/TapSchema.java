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
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.impl.java.MapSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TapSchema extends MapSchema
  {
  private static final Logger LOG = LoggerFactory.getLogger( TapSchema.class );

  private final LingualConnection connection;
  private final PlatformBroker platformBroker;
  private final String name;
  private final String identifier;

  public TapSchema( LingualConnection connection, SchemaDef schemaDef )
    {
    this( connection, schemaDef.getName(), schemaDef.getIdentifier() );
    }

  public TapSchema( LingualConnection connection, String name, String identifier )
    {
    super( connection.getParent(), connection.getTypeFactory(), makeExpression( connection, name ) );
    this.connection = connection;
    this.platformBroker = connection.getPlatformBroker();
    this.name = name;
    this.identifier = identifier;
    }

  public String getName()
    {
    return name;
    }

  public String getIdentifier()
    {
    return identifier;
    }

  private static Expression makeExpression( LingualConnection connection, String name )
    {
    return connection.getRootSchema().getSubSchemaExpression( name, Object.class );
    }

  public void addTapTablesFor( SchemaDef schemaDef )
    {
    for( TableDef tableDef : schemaDef.getChildTables() )
      {
      addTapTableFor( tableDef );
      }
    }

  public void addTapTableFor( TableDef tableDef )
    {
    if( getTable( tableDef.getName() ) != null )
      {
      return;
      }

    TapTable table = new TapTable( platformBroker, getQueryProvider(), this, tableDef );

    LOG.info( "on schema: {}, adding table: {}, with fields: {}",
      new Object[]{getName(), table.getName(), table.getFields()} );

    addTable( table.getName(), table );
    }
  }
