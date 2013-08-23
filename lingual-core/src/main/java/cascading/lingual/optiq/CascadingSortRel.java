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

import java.util.Comparator;

import cascading.lingual.optiq.meta.Branch;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import net.hydromatic.linq4j.function.Functions;
import org.eigenbase.rel.RelCollation;
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
class CascadingSortRel extends SortRel implements CascadingRelNode
  {
  public CascadingSortRel( RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation, RexNode offset, RexNode fetch )
    {
    super( cluster, traits, child, collation, offset, fetch );

    assert child.getTraitSet().contains( Cascading.CONVENTION );
    assert offset == null;
    assert fetch == null;
    }

  @Override
  public RelOptCost computeSelfCost( RelOptPlanner planner )
    {
    return super.computeSelfCost( planner ).multiplyBy( .1 );
    }

  @Override
  public SortRel copy( RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch )
    {
    return new CascadingSortRel( getCluster(), traitSet, newInput, newCollation, offset, fetch );
    }

  public Branch visitChild( Stack stack )
    {
    Branch branch = ( (CascadingRelNode) getChild() ).visitChild( stack );
    Fields fields = createFields();

    String name = stack.getNameFor( GroupBy.class, branch.current );
    Pipe current = new GroupBy( name, branch.current, fields );

    current = stack.addDebug( this, current );

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

    for( RelFieldCollation fieldCollation : collation.getFieldCollations() )
      {
      String name = inputRowType.getFieldList().get( fieldCollation.getFieldIndex() ).getName();
      boolean isDescending = fieldCollation.getDirection() == RelFieldCollation.Direction.Descending;
      boolean isNullsFirst = fieldCollation.nullDirection == RelFieldCollation.NullDirection.FIRST;

      Comparator<Comparable> comparator = Functions.<Comparable>nullsComparator( isNullsFirst, isDescending );

      if( comparator != null )
        fields.setComparator( name, comparator );
      }

    return fields;
    }
  }
