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

import java.util.List;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.UnionRel;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Rule that converts a logical join rel to a cascading join rel. */
class CascadingUnionRule extends RelOptRule
  {
  private static final Logger LOG = LoggerFactory.getLogger( CascadingUnionRule.class );

  public static final CascadingUnionRule INSTANCE = new CascadingUnionRule();

  public CascadingUnionRule()
    {
    super(
      some(
        UnionRel.class,
        any( RelNode.class, Cascading.CONVENTION ),
        any( RelNode.class, Cascading.CONVENTION ) ),
      "cascading groupby merge" );
    }

  @Override
  public void onMatch( RelOptRuleCall call )
    {
    List<RelNode> rels = call.getRelList();

    final UnionRel union = (UnionRel) rels.get( 0 );
    final List<RelNode> inputs = rels.subList( 1, rels.size() );

    if( !union.getVariablesStopped().isEmpty() )
      {
      LOG.warn( "variables stopped not supported by this rule" );
      return;
      }

    call.transformTo(
      new CascadingUnionRel(
        union.getCluster(),
        union.getCluster().traitSetOf( Cascading.CONVENTION ),
        inputs,
        union.all
      ) );
    }
  }


