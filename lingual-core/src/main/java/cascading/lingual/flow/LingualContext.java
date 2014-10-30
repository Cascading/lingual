/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.lingual.flow;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import cascading.bind.catalog.Stereotype;
import cascading.flow.Flow;
import cascading.lingual.catalog.SchemaDef;
import cascading.lingual.catalog.TableDef;
import cascading.lingual.optiq.FieldTypeFactory;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.tap.TapSchema;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.google.common.base.Function;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.ConnectionProperty;
import net.hydromatic.optiq.jdbc.OptiqPrepare;

/**
 *
 */
class LingualContext implements OptiqPrepare.Context
  {
  private final SQLPlanner sqlPlanner;
  private final TapSchema rootMapSchema;

  LingualContext( SQLPlanner sqlPlanner, Flow flowDef, PlatformBroker platformBroker )
    {
    this.sqlPlanner = sqlPlanner;
    this.rootMapSchema = new TapSchema( new FlowQueryProvider(), new FieldTypeFactory(), platformBroker );

    initializeSchema( sqlPlanner, flowDef, rootMapSchema );
    }

  private void initializeSchema( SQLPlanner sqlPlanner, Flow flowDef, TapSchema currentTapSchema )
    {
    if( sqlPlanner.getDefaultSchema() != null )
      currentTapSchema = createGetTapSchema( currentTapSchema, getDefaultSchema() );

    SchemaDef currentSchemaDef = new SchemaDef();

    if( sqlPlanner.getDefaultSchema() != null )
      currentSchemaDef = createGetSchemaDef( currentSchemaDef, getDefaultSchema() );

    addTaps( currentSchemaDef, currentTapSchema, flowDef.getSources(), new Function<Tap, Fields>()
    {
    @Override
    public Fields apply( Tap input )
      {
      return input.getSourceFields();
      }
    } );

    addTaps( currentSchemaDef, currentTapSchema, flowDef.getSinks(), new Function<Tap, Fields>()
    {
    @Override
    public Fields apply( Tap input )
      {
      return input.getSinkFields();
      }
    } );
    }

  @Override
  public JavaTypeFactory getTypeFactory()
    {
    return new FieldTypeFactory();
    }

  @Override
  public Schema getRootSchema()
    {
    return rootMapSchema;
    }

  private String getDefaultSchema()
    {
    return sqlPlanner.getDefaultSchema();
    }

  @Override
  public List<String> getDefaultSchemaPath()
    {
    if( getDefaultSchema() == null )
      return Collections.emptyList();

    return Arrays.asList( getDefaultSchema().split( "\\." ) );
    }

  @Override
  public ConnectionProperty.ConnectionConfig config()
    {
    return ConnectionProperty.connectionConfig( new Properties() );
    }

  private void addTaps( SchemaDef parentSchemaDef, TapSchema parentTapSchema, Map<String, Tap> taps, Function<Tap, Fields> function )
    {
    for( String name : taps.keySet() )
      {
      TapSchema currentTapSchema = parentTapSchema;
      SchemaDef currentSchemaDef = parentSchemaDef;
      Tap tap = taps.get( name );
      String[] split = name.split( "\\." );

      for( int i = 0; i < split.length - 1; i++ )
        {
        currentTapSchema = createGetTapSchema( currentTapSchema, split[ i ] );
        currentSchemaDef = createGetSchemaDef( currentSchemaDef, split[ i ] );
        }

      name = split[ split.length - 1 ];

      Stereotype stereotype = new Stereotype( name, function.apply( tap ) );
      TableDef tableDef = new TableDef( currentSchemaDef, name, tap.getIdentifier(), stereotype );

      currentSchemaDef.addStereotype( stereotype );
      currentTapSchema.addTapTableFor( tableDef, getDefaultSchema() == null );
      }
    }

  private SchemaDef createGetSchemaDef( SchemaDef parentSchemaDef, String schemaName )
    {
    if( parentSchemaDef.getSchema( schemaName ) == null )
      parentSchemaDef.addSchema( schemaName, null, null, null );

    return parentSchemaDef.getSchema( schemaName );
    }

  private TapSchema createGetTapSchema( TapSchema parentSchema, String schemaName )
    {
    if( parentSchema.getSubSchema( schemaName ) == null )
      parentSchema.addSchema( schemaName, new TapSchema( parentSchema, schemaName ) );

    return (TapSchema) parentSchema.getSubSchema( schemaName );
    }

  }
