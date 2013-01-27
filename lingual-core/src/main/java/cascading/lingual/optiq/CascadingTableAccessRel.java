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

import cascading.lingual.optiq.meta.Branch;
import cascading.lingual.platform.PlatformBroker;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.volcano.AbstractConverter;

/**
 *
 */
public class CascadingTableAccessRel extends TableAccessRelBase implements CascadingRelNode
  {
  private final PlatformBroker platformBroker;
  private final String name;
  private final String identifier;

  public CascadingTableAccessRel( RelOptCluster cluster, RelOptTable table, PlatformBroker platformBroker, String name, String identifier )
    {
    super( cluster, cluster.getEmptyTraitSet().plus( Cascading.CONVENTION ), table );
    this.platformBroker = platformBroker;
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
    // handles actual flow planning
    planner.addRule( CascadingEnumerableConverterRule.ARRAY_INSTANCE );
    planner.addRule( CascadingEnumerableConverterRule.CUSTOM_INSTANCE );

    planner.addRule( CascadingTableModificationConverterRule.INSTANCE );
    planner.addRule( CascadingAggregateConverterRule.INSTANCE );
    planner.addRule( CascadingCalcConverterRule.INSTANCE );

//    planner.addRule( CascadingFilterRule.INSTANCE );
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
    return new Branch( platformBroker, stack.heads, name, identifier );
    }
  }
