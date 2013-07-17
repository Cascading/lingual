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

package cascading.lingual.optiq;

import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.TableDef;
import cascading.lingual.optiq.meta.Branch;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.tap.TapTable;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.type.FileType;
import cascading.util.Util;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.rel.rules.PushJoinThroughJoinRule;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.volcano.AbstractConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CascadingTableAccessRel extends TableAccessRelBase implements CascadingRelNode
  {
  private static final Logger LOG = LoggerFactory.getLogger( CascadingTableAccessRel.class );

  final String name;
  final String identifier;

  private Double rows; // cache the value, its costly to get

  public CascadingTableAccessRel( RelOptCluster cluster, RelOptTable table, String name, String identifier )
    {
    super( cluster, cluster.traitSetOf( Cascading.CONVENTION ), table );
    this.name = name;
    this.identifier = identifier;
    }

  @Override
  public void register( RelOptPlanner planner )
    {
    super.register( planner );
    registerRules( planner );
    }

  public static void registerRules( RelOptPlanner planner )
    {
    planner.addRule( PushJoinThroughJoinRule.INSTANCE );

    // handles actual flow planning
    planner.addRule( CascadingEnumerableConverterRule.INSTANCE );

    planner.addRule( CascadingTableModificationConverterRule.INSTANCE );
    planner.addRule( CascadingAggregateConverterRule.INSTANCE );
    planner.addRule( CascadingCalcConverterRule.INSTANCE );
    planner.addRule( CascadingProjectConverterRule.INSTANCE );

//    planner.addRule( CascadingFilterRule.INSTANCE );
    planner.addRule( CascadingAggregateRule.INSTANCE );
    planner.addRule( CascadingProjectRule.INSTANCE );
    planner.addRule( CascadingSortRule.INSTANCE );
    planner.addRule( CascadingJoinRule.INSTANCE );
    planner.addRule( CascadingUnionRule.INSTANCE );
    planner.addRule( CascadingValuesRule.INSTANCE );
    planner.addRule( CascadingInsertValuesRule.INSTANCE );
    planner.addRule( AbstractConverter.ExpandConversionRule.instance );
    }

  public Branch visitChild( Stack stack )
    {
    String pipeName = isUseFullName() ? getQualifiedName() : name;

    return new Branch( getPlatformBroker(), stack.heads, pipeName, getTapTable().getTableDef() );
    }

  private String getQualifiedName()
    {
    return Util.join( table.getQualifiedName(), "." );
    }

  public PlatformBroker getPlatformBroker()
    {
    return getTapTable().getPlatformBroker();
    }

  public boolean isUseFullName()
    {
    return getTapTable().isUseFullName();
    }

  private TapTable getTapTable()
    {
    return getTable().unwrap( TapTable.class );
    }

  @Override
  public synchronized double getRows()
    {
    if( rows == null )
      rows = getRowsInternal();

    return rows;
    }

  private double getRowsInternal()
    {
    PlatformBroker platformBroker = getPlatformBroker();

    if( platformBroker == null )
      return super.getRows();

    SchemaCatalog catalog = platformBroker.getCatalog();

    if( catalog == null )
      return super.getRows();

    TableDef tableDef = getTapTable().getTableDef();

    try
      {
      Tap tap = catalog.createTapFor( tableDef, SinkMode.KEEP );

      if( tap != null )
        {
        if( !tap.resourceExists( platformBroker.getSystemConfig() ) )
          return 0.0;
        else if( tap instanceof FileType )
          return ( (FileType) tap ).getSize( platformBroker.getSystemConfig() ); // actually returns bytes
        }
      }
    catch( Exception exception )
      {
      LOG.warn( "unable to create tap for: " + tableDef );
      }

    return super.getRows();
    }
  }
