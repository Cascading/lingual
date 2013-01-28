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
import java.util.Map;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.jdbc.Driver;
import cascading.lingual.optiq.enumerable.CascadingFlowRunnerEnumerable;
import cascading.lingual.optiq.enumerable.CascadingValueInsertEnumerable;
import cascading.lingual.optiq.meta.Branch;
import cascading.lingual.optiq.meta.FlowHolder;
import cascading.lingual.optiq.meta.Ref;
import cascading.lingual.optiq.meta.ValuesHolder;
import cascading.lingual.platform.LingualFlowFactory;
import cascading.lingual.platform.PlatformBroker;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import net.hydromatic.linq4j.expressions.BlockBuilder;
import net.hydromatic.linq4j.expressions.BlockExpression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.PhysType;
import net.hydromatic.optiq.rules.java.PhysTypeImpl;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexLiteral;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CascadingEnumerableRel extends SingleRel implements EnumerableRel
  {
  private static final Logger LOG = LoggerFactory.getLogger( CascadingEnumerableRel.class );

  private PhysType physType;

  public CascadingEnumerableRel( RelOptCluster cluster, RelTraitSet traitSet, RelNode input )
    {
    super( cluster, traitSet, input );
    assert getConvention() instanceof EnumerableConvention;
    physType = PhysTypeImpl.of( (JavaTypeFactory) cluster.getTypeFactory(), input.getRowType(), (EnumerableConvention) getConvention() );
    }

  public PhysType getPhysType()
    {
    return physType;
    }

  @Override
  public RelOptCost computeSelfCost( RelOptPlanner planner )
    {
    return super.computeSelfCost( planner ).multiplyBy( .1 );
    }

  @Override
  public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs )
    {
    return new CascadingEnumerableRel( getCluster(), traitSet, sole( inputs ) );
    }

  public BlockExpression implement( EnumerableRelImplementor implementor )
    {
    LOG.debug( "implementing enumerable" );

    CascadingRelNode input = (CascadingRelNode) getChild();
    Branch branch = input.visitChild( new Stack() );
    PlatformBroker platformBroker = branch.platformBroker;

    if( platformBroker == null )
      {
      throw new IllegalStateException( "platformBroker was null" );
      }

    if( branch.tuples != null )
      {
      return handleInsert( platformBroker, branch );
      }
    else
      {
      return handleFlow( platformBroker, branch );
      }
    }

  private BlockExpression handleInsert( PlatformBroker platformBroker, Branch branch )
    {
    FlowProcess flowProcess = platformBroker.getFlowProcess();
    SchemaCatalog schemaCatalog = platformBroker.getCatalog();
    Map<String, TupleEntryCollector> cache = platformBroker.getCollectorCache();

    List<List<RexLiteral>> tuples = branch.tuples;
    Tap tap = schemaCatalog.createTapFor( branch.tail.identifier, SinkMode.KEEP );
    ValuesHolder holder = new ValuesHolder( cache, flowProcess, tap, tuples );

    int ordinal = CascadingValueInsertEnumerable.addHolder( holder );

    Constructor<CascadingValueInsertEnumerable> constructor = getConstructorFor( CascadingValueInsertEnumerable.class );

    return new BlockBuilder().append( Expressions.new_( constructor, Expressions.constant( ordinal ) ) ).toBlock();
    }

  private BlockExpression handleFlow( PlatformBroker platformBroker, Branch branch )
    {
    Properties properties = platformBroker.getProperties();
    LingualFlowFactory flowFactory = platformBroker.getFlowFactory( branch );

    for( Ref head : branch.heads.keySet() )
      {
      flowFactory.addSource( head.name, head.identifier );
      }

    if( branch.tail != null )
      {
      flowFactory.addSink( branch.tail.name, branch.tail.identifier );
      }
    else
      {
      flowFactory.addSink( branch.current.getName(), getResultPath( platformBroker, properties, flowFactory.getName() ) );
      }

    FlowHolder flowHolder = new FlowHolder( flowFactory, branch.isModification );

    setDotPath( properties, flowFactory.getName(), flowHolder );

    int ordinal = CascadingFlowRunnerEnumerable.addHolder( flowHolder );

    Constructor<CascadingFlowRunnerEnumerable> constructor = getConstructorFor( CascadingFlowRunnerEnumerable.class );

    return new BlockBuilder().append( Expressions.new_( constructor, Expressions.constant( ordinal ) ) ).toBlock();
    }

  private String getResultPath( PlatformBroker platformBroker, Properties properties, String name )
    {
    String path = platformBroker.getTempPath();
    path = properties.getProperty( Driver.RESULT_PATH_PROP, path );

    if( !path.endsWith( "/" ) )
      {
      path += "/";
      }

    return path + name;
    }

  private void setDotPath( Properties properties, String name, FlowHolder flowHolder )
    {
    if( !properties.containsKey( Driver.DOT_PATH_PROP ) )
      {
      return;
      }

    flowHolder.dotPath = properties.getProperty( Driver.DOT_PATH_PROP );

    if( !flowHolder.dotPath.endsWith( "/" ) )
      {
      flowHolder.dotPath += "/";
      }

    flowHolder.dotPath += name + ".dot";
    }

  private <T> Constructor<T> getConstructorFor( Class<T> type )
    {
    Constructor<T> constructor;

    try
      {
      constructor = type.getConstructor( int.class );
      }
    catch( NoSuchMethodException exception )
      {
      throw new RuntimeException( exception );
      }

    return constructor;
    }
  }
