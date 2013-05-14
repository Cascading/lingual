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
import java.util.Collections;
import java.util.List;

import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Rule that converts a logical join rel to a cascading join rel. */
class CascadingJoinRule extends RelOptRule
  {
  private static final Logger LOG = LoggerFactory.getLogger( CascadingJoinRule.class );

  public static final CascadingJoinRule INSTANCE =
    new CascadingJoinRule();

  public CascadingJoinRule()
    {
    super(
      new RelOptRuleOperand( JoinRel.class,
        new RelOptRuleOperand( RelNode.class, Cascading.CONVENTION ),
        new RelOptRuleOperand( RelNode.class, Cascading.CONVENTION ) ),
      "cascading cogroup join" );
    }

  @Override
  public void onMatch( RelOptRuleCall call )
    {
    RelNode[] rels = call.getRels();

    assert rels.length == 3;

    final JoinRel join = (JoinRel) rels[ 0 ];
    final RelNode left = rels[ 1 ];
    final RelNode right = rels[ 2 ];

    if( !join.getVariablesStopped().isEmpty() )
      {
      LOG.debug( "variables stopped not supported by this rule" );
      return;
      }

    // split into equi join
    final List<RexNode> leftExprs = new ArrayList<RexNode>();
    final List<RexNode> rightExprs = new ArrayList<RexNode>();

    RexNode remainder =
      RelOptUtil.splitJoinCondition(
        Collections.<RelDataTypeField>emptyList(),
        left,
        right,
        join.getCondition(),
        leftExprs,
        rightExprs,
        new ArrayList<Integer>(),
        null );

    if( remainder != null )
      {
      LOG.debug( "cannot handle non-equi join" );
      return;
      }

    final List<Integer> leftKeys = new ArrayList<Integer>();
    final List<Integer> rightKeys = new ArrayList<Integer>();
    final List<Integer> outputProj = new ArrayList<Integer>();
    final RelNode[] inputRels = {left, right};

    RelOptUtil.projectJoinInputs(
      inputRels,
      leftExprs,
      rightExprs,
      0,
      leftKeys,
      rightKeys,
      outputProj );

    final RelNode newLeft = inputRels[ 0 ];
    final RelNode newRight = inputRels[ 1 ];
    final RelTraitSet traits = join.getCluster().getEmptyTraitSet().plus( Cascading.CONVENTION );
    final RexNode newCondition = createCondition( join.getCluster().getRexBuilder(), newLeft, leftKeys, newRight, rightKeys );

    final CascadingJoinRel newJoin = new CascadingJoinRel( join.getCluster(), traits, convert( newLeft, traits ),
      convert( newRight, traits ), newCondition, join.getJoinType(), join.getVariablesStopped(), 0 );

    final RelNode newRel = convert( RelOptUtil.createProjectJoinRel( outputProj, newJoin ), traits );

    call.transformTo( newRel );
    }

  private RexNode createCondition( RexBuilder builder,
                                   RelNode left,
                                   List<Integer> leftKeys,
                                   RelNode right,
                                   List<Integer> rightKeys )
    {
    final List<RelDataType> leftTypes = RelOptUtil.getFieldTypeList( left.getRowType() );
    final List<RelDataType> rightTypes = RelOptUtil.getFieldTypeList( right.getRowType() );
    final List<RexNode> exprs = new ArrayList<RexNode>();

    for( Pair<Integer, Integer> pair : Pair.zip( leftKeys, rightKeys ) )
      exprs.add(
        builder.makeCall(
          SqlStdOperatorTable.equalsOperator,
          builder.makeInputRef( leftTypes.get( pair.left ), pair.left ),
          builder.makeInputRef( rightTypes.get( pair.right ), pair.right + leftTypes.size() ) ) );

    return RexUtil.andRexNodeList( builder, exprs );
    }
  }


