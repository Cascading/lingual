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
import java.util.Properties;

import cascading.lingual.jdbc.Driver;
import cascading.lingual.optiq.enumerable.CascadingEnumerable;
import cascading.lingual.optiq.meta.Branch;
import cascading.lingual.optiq.meta.Holder;
import cascading.lingual.optiq.meta.Ref;
import cascading.lingual.platform.LingualFlowFactory;
import cascading.lingual.platform.PlatformBroker;
import net.hydromatic.linq4j.expressions.BlockBuilder;
import net.hydromatic.linq4j.expressions.BlockExpression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.JavaRules;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CascadingEnumerableRel extends SingleRel implements EnumerableRel
  {
  private static final Logger LOG = LoggerFactory.getLogger( CascadingEnumerableRel.class );

  public CascadingEnumerableRel( RelOptCluster cluster, RelTraitSet traitSet, RelNode input )
    {
    super( cluster, traitSet.plus( JavaRules.CONVENTION ), input );
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

    Stack stack = new Stack();
    CascadingRelNode input = (CascadingRelNode) getChild();

    Branch branch = input.visitChild( stack );
    PlatformBroker platformBroker = branch.platformBroker;

    Properties properties = platformBroker.getProperties();
    LingualFlowFactory flowFactory = platformBroker.getFlowFactory( branch );

    for( Ref head : branch.heads.keySet() )
      flowFactory.addSource( head.name, head.identifier );

    if( branch.tail != null )
      flowFactory.addSink( branch.tail.name, branch.tail.identifier );
    else
      flowFactory.addSink( branch.current.getName(), getResultPath( platformBroker, properties, flowFactory.getName() ) );

    Holder holder = new Holder( flowFactory );

    setDotPath( properties, flowFactory.getName(), holder );

    int ordinal = CascadingEnumerable.addHolder( holder );

    Constructor<CascadingEnumerable> constructor = getConstructorFor( CascadingEnumerable.class );

    return new BlockBuilder().append( Expressions.new_( constructor, Expressions.constant( ordinal ) ) ).toBlock();
    }

  private String getResultPath( PlatformBroker platformBroker, Properties properties, String name )
    {
    String path = platformBroker.getTempPath();
    path = properties.getProperty( Driver.RESULT_PATH_PROP, path );

    if( !path.endsWith( "/" ) )
      path += "/";

    return path + name;
    }

  private void setDotPath( Properties properties, String name, Holder holder )
    {
    if( !properties.containsKey( Driver.DOT_PATH_PROP ) )
      return;

    holder.dotPath = properties.getProperty( Driver.DOT_PATH_PROP );

    if( !holder.dotPath.endsWith( "/" ) )
      holder.dotPath += "/";

    holder.dotPath += name + ".dot";
    }

  private Constructor<CascadingEnumerable> getConstructorFor( Class<CascadingEnumerable> type )
    {
    Constructor<CascadingEnumerable> constructor;

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
