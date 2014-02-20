/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

import cascading.lingual.optiq.meta.Branch;
import org.eigenbase.rel.CalcRelBase;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexProgram;

/**
 *
 */
class CascadingCalcRel extends CalcRelBase implements CascadingRelNode
  {
  public CascadingCalcRel( RelOptCluster cluster, RelTraitSet traits, RelNode child, RelDataType rowType, RexProgram program, List<RelCollation> collationList )
    {
    super( cluster, traits.plus( Cascading.CONVENTION ), child, rowType, program, collationList );
    }

  @Override
  public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs )
    {
    return new CascadingCalcRel( getCluster(), getTraitSet(), sole( inputs ), getRowType(), getProgram(), getCollationList() );
    }

  @Override
  public RelOptCost computeSelfCost( RelOptPlanner planner )
    {
    return super.computeSelfCost( planner ).multiplyBy( .1 );
    }

  @Override
  public Branch visitChild( Stack stack )
    {
    return CalcProjectUtil.resolveBranch( stack, this, getProgram() );
    }
  }
