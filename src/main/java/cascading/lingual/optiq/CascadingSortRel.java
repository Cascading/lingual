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

import java.util.Collections;
import java.util.List;

import cascading.lingual.optiq.meta.Branch;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;

/**
 *
 */
public class CascadingSortRel extends SortRel implements CascadingRelNode
  {
  public CascadingSortRel( RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RelFieldCollation> collations )
    {
    super( cluster, traits, child, collations );

    assert child.getTraitSet().contains( CascadingCallingConvention.CASCADING );
    }

  @Override
  public RelOptCost computeSelfCost( RelOptPlanner planner )
    {
    return super.computeSelfCost( planner ).multiplyBy( .1 );
    }

  @Override
  public SortRel copy( RelTraitSet traitSet, RelNode newInput, List<RelFieldCollation> newCollations )
    {
    return new CascadingSortRel( getCluster(), traitSet, newInput, newCollations );
    }

  public Branch visitChild( Stack stack )
    {
    Branch branch = ( (CascadingRelNode) getChild() ).visitChild( stack );
    Fields fields = createFields();

    Pipe current = new GroupBy( branch.current, fields );

    return new Branch( current, branch );
    }

  private Fields createFields()
    {
    Fields fields = new Fields();
    RelNode child = getChild();
    RelDataType inputRowType = child.getRowType();

    for( RexNode exp : fieldExps )
      {
      int index = ( (RexInputRef) exp ).getIndex();
      RelDataTypeField typeField = inputRowType.getFieldList().get( index );
      String name = typeField.getName();

      fields = fields.append( new Fields( name ) );
      }

    for( RelFieldCollation collation : collations )
      {
      if( collation.getDirection() == RelFieldCollation.Direction.Descending )
        fields.setComparator( collation.getFieldIndex(), Collections.reverseOrder() );
      }

    return fields;
    }
  }
