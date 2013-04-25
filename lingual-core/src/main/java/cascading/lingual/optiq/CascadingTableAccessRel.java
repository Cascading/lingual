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

import java.util.HashMap;
import java.util.Map;

import cascading.lingual.optiq.meta.Branch;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.tap.TapTable;
import cascading.util.Util;
import net.hydromatic.optiq.rules.java.JavaRules;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.rel.rules.PushJoinThroughJoinRule;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.volcano.AbstractConverter;

/**
 *
 */
public class CascadingTableAccessRel extends TableAccessRelBase implements CascadingRelNode
  {
  private final String name;
  private final String identifier;

  public CascadingTableAccessRel( RelOptCluster cluster, RelOptTable table, String name, String identifier )
    {
    super( cluster, cluster.getEmptyTraitSet().plus( Cascading.CONVENTION ), table );
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
    planner.removeRule( JavaRules.ENUMERABLE_CUSTOM_FROM_ARRAY_RULE );

    planner.addRule( PushJoinThroughJoinRule.INSTANCE );

    // handles actual flow planning
    planner.addRule( CascadingEnumerableConverterRule.ARRAY_INSTANCE );

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

    return new Branch( getPlatformBroker(), stack.heads, pipeName, identifier );
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

  // todo: remove
  private static final Map<String, Double> TABLE_ROW_COUNTS =
    new HashMap<String, Double>()
    {
    {
    put( "time_by_day", 730d );
    put( "inventory_fact_1997", 4070d );
    put( "sales_fact_1997", 86837d );
    put( "customer", 10281d );
    put( "product", 1560d );
    put( "product_class", 110d );
    put( "promotion", 1864d );
    put( "store", 25d );
    put( "warehouse", 24d );
    }
    };

  @Override
  public double getRows()
    {
    // Hard-coded row-counts for our test tables. TODO: get file sizes from HDFS
    final String[] names = table.getQualifiedName();
    final String name = names[ names.length - 1 ];
    final Double n = TABLE_ROW_COUNTS.get( name );
    if( n != null )
      return n.doubleValue();
    return super.getRows();
    }
  }
