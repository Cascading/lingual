/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Rule that converts a logical join rel to a cascading join rel. */
public class CascadingJoinRule extends RelOptRule
  {
  private static final Logger LOG = LoggerFactory.getLogger( CascadingJoinRule.class );

  public static final CascadingJoinRule INSTANCE =
    new CascadingJoinRule();

  public CascadingJoinRule()
    {
    super(
      new RelOptRuleOperand( JoinRel.class,
        new RelOptRuleOperand( RelNode.class, CascadingConvention.CASCADING ),
        new RelOptRuleOperand( RelNode.class, CascadingConvention.CASCADING ) ),
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
      LOG.warn( "variables stopped not supported by this rule" );
      return;
      }

    call.transformTo(
      new CascadingJoinRel(
        join.getCluster(),
        join.getCluster().getEmptyTraitSet().plus( CascadingConvention.CASCADING ),
        left,
        right,
        join.getCondition(),
        join.getJoinType(),
        join.getVariablesStopped(),
        0 ) );
    }
  }


