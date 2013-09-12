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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import cascading.lingual.optiq.meta.Branch;
import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.Joiner;
import cascading.pipe.joiner.LeftJoin;
import cascading.pipe.joiner.OuterJoin;
import cascading.pipe.joiner.RightJoin;
import cascading.tuple.Fields;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

import static cascading.lingual.optiq.RelUtil.createTypedFieldsSelectorFor;

/** Join implemented in Cascading. */
class CascadingJoinRel extends JoinRelBase implements CascadingRelNode
  {
  /** Whether a hash join. 0 = not, 1 = hash join builds on the left, 2 = hash join builds on the right. */
  private final int hash;
  private final List<Integer> leftKeys = new ArrayList<Integer>();
  private final List<Integer> rightKeys = new ArrayList<Integer>();

  public CascadingJoinRel( RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, JoinRelType joinType, Set<String> variablesStopped, int hash )
    {
    super( cluster, traits, left, right, condition, joinType, variablesStopped );
    this.hash = hash;

    RexNode remaining = RelOptUtil.splitJoinCondition( left, right, condition, leftKeys, rightKeys );

    // Rule should have checked "isEqui" before firing. Something went wrong.
    if( !remaining.isAlwaysTrue() )
      throw new AssertionError( "not equi-join condition: " + remaining );
    }

  @Override
  public RelOptCost computeSelfCost( RelOptPlanner planner )
    {
    final RelOptCost cost = super.computeSelfCost( planner );

    if( leftKeys.size() == 0 ) // cartesian product. make artificially expensive.
      return cost.multiplyBy( 10d );

    return cost.multiplyBy( .1 );
    }

  @Override
  public RelOptPlanWriter explainTerms( RelOptPlanWriter pw )
    {
    return super.explainTerms( pw )
      .item( "hash", hash );
    }

  @Override
  public JoinRelBase copy( RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right )
    {
    return new CascadingJoinRel( getCluster(), traitSet, left, right, conditionExpr, this.joinType, this.variablesStopped, this.hash );
    }

  public Branch visitChild( Stack stack )
    {
    Branch lhsBranch = ( (CascadingRelNode) left ).visitChild( stack );
    Branch rhsBranch = ( (CascadingRelNode) right ).visitChild( stack );

    Pipe leftPipe = new Pipe( "lhs", lhsBranch.current );
    leftPipe = stack.addDebug( this, leftPipe, "lhs" );

    Pipe rightPipe = new Pipe( "rhs", rhsBranch.current );
    rightPipe = stack.addDebug( this, rightPipe, "rhs" );

    Fields lhsGroup = createTypedFieldsSelectorFor( getCluster(), leftKeys, left.getRowType(), true );
    Fields rhsGroup = createTypedFieldsSelectorFor( getCluster(), rightKeys, right.getRowType(), true );

    Joiner joiner = getJoiner();

    Fields declaredFields = RelUtil.createTypedFieldsFor( this, false );

    // need to parse lhs rhs fields from condition
    String name = stack.getNameFor( CoGroup.class, leftPipe, rightPipe );
    Pipe coGroup = new CoGroup( name, leftPipe, lhsGroup, rightPipe, rhsGroup, declaredFields, joiner );

    coGroup = stack.addDebug( this, coGroup );

    return new Branch( coGroup, lhsBranch, rhsBranch );
    }

  private Joiner getJoiner()
    {
    switch( getJoinType() )
      {
      case INNER:
        return new InnerJoin();
      case LEFT:
        return new LeftJoin();
      case RIGHT:
        return new RightJoin();
      case FULL:
        return new OuterJoin();
      default:
        throw new IllegalStateException( "unknown join type" );
      }
    }
  }
