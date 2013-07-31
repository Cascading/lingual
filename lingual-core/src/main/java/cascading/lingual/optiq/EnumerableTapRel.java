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

import java.lang.reflect.Constructor;
import java.util.List;

import cascading.lingual.catalog.TableDef;
import cascading.lingual.optiq.enumerable.CascadingTapEnumerable;
import cascading.lingual.optiq.meta.TableHolder;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.tap.TapTable;
import net.hydromatic.linq4j.expressions.BlockBuilder;
import net.hydromatic.linq4j.expressions.BlockStatement;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.JavaRowFormat;
import net.hydromatic.optiq.rules.java.PhysType;
import net.hydromatic.optiq.rules.java.PhysTypeImpl;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.relopt.volcano.VolcanoPlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Relational expression that reads from a Cascading tap and returns in
 * enumerable format.
 */
class EnumerableTapRel extends TableAccessRelBase implements EnumerableRel
  {
  private static final Logger LOG = LoggerFactory.getLogger( EnumerableTapRel.class );

  public EnumerableTapRel( RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table )
    {
    super( cluster, traitSet, table );

    if( getConvention() != EnumerableConvention.INSTANCE )
      throw new IllegalStateException( "unsupported convention " + getConvention() );
    }

  @Override
  public RelOptCost computeSelfCost( RelOptPlanner planner )
    {
    return super.computeSelfCost( planner ).multiplyBy( .1 );
    }

  @Override
  public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs )
    {
    assert inputs.isEmpty();
    return new EnumerableTapRel( getCluster(), traitSet, table );
    }

  @Override
  public Result implement( EnumerableRelImplementor implementor, Prefer pref )
    {
    LOG.debug( "implementing enumerable" );

    TableDef tableDef = getTapTable().getTableDef();

    VolcanoPlanner planner = (VolcanoPlanner) getCluster().getPlanner();

    if( pref == Prefer.CUSTOM )
      throw new RuntimeException( "cannot return custom format" );

    final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), table.getRowType(), JavaRowFormat.ARRAY );

    TableHolder tableHolder = new TableHolder( physType, tableDef, getPlatformBroker(), planner );
    long ordinal = CascadingTapEnumerable.addHolder( tableHolder );
    Constructor<CascadingTapEnumerable> constructor = CascadingEnumerableRel.getConstructorFor( CascadingTapEnumerable.class );

    BlockStatement block = new BlockBuilder().append( Expressions.new_( constructor, Expressions.constant( ordinal ) ) ).toBlock();
    return implementor.result( physType, block );
    }

  private PlatformBroker getPlatformBroker()
    {
    return getTapTable().getPlatformBroker();
    }

  private TapTable getTapTable()
    {
    return getTable().unwrap( TapTable.class );
    }
  }
