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

import java.util.List;

import cascading.lingual.optiq.meta.Branch;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Unique;
import cascading.tuple.Fields;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SetOpRel;
import org.eigenbase.rel.UnionRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

/** Join implemented in Cascading. */
public class CascadingUnionRel extends UnionRelBase implements CascadingRelNode
  {
  public CascadingUnionRel( RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all )
    {
    super( cluster, traits, inputs, all );
    }

  @Override
  public RelOptCost computeSelfCost( RelOptPlanner planner )
    {
    return super.computeSelfCost( planner ).multiplyBy( .1 );
    }

//  @Override
//  public void explain( RelOptPlanWriter pw )
//    {
//    Yuck. Add a builder pattern so we can just call super.
//    final List<String> nameList = new ArrayList<String>( Arrays.asList( "left", "right", "condition" ) );
//    final List<Object> valueList = new ArrayList<Object>();
//
//    nameList.add( "joinType" );
//    valueList.add( joinType.name().toLowerCase() );
//    nameList.add( "hash" );
//    valueList.add( hash );
//
//    if( !getSystemFieldList().isEmpty() )
//      {
//      nameList.add( "systemFields" );
//      valueList.add( getSystemFieldList() );
//      }
//
//    pw.explain( this, nameList, valueList );
//    }

  @Override
  public SetOpRel copy( RelTraitSet traitSet, List<RelNode> inputs, boolean all )
    {
    return new CascadingUnionRel( getCluster(), traitSet, inputs, all );
    }

  public Branch visitChild( Stack stack )
    {
    Pipe[] pipes = new Pipe[ inputs.size() ];
    Branch[] branches = new Branch[ inputs.size() ];

    for( int i = 0; i < inputs.size(); i++ )
      {
      branches[ i ] = ( (CascadingRelNode) inputs.get( i ) ).visitChild( stack );
      pipes[ i ] = branches[ i ].current;
      }

    Pipe pipe;

    if( !all )
      pipe = new Unique( pipes, Fields.ALL );
    else
      pipe = new GroupBy( pipes, Fields.ALL );

    return new Branch( pipe, branches );
    }
  }
