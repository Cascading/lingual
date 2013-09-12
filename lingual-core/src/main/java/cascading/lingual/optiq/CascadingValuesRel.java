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

import cascading.lingual.optiq.meta.Branch;
import cascading.tuple.Fields;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.ValuesRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexLiteral;

import static cascading.lingual.util.Misc.createUniqueName;

/**
 * Implementation of VALUES operator, that returns a constant set of tuples,
 * in Cascading convention.
 */
class CascadingValuesRel extends ValuesRelBase implements CascadingRelNode
  {
  public CascadingValuesRel(
    RelOptCluster cluster,
    RelTraitSet traits,
    RelDataType rowType,
    List<List<RexLiteral>> tuples )
    {
    super( cluster, rowType, tuples, traits );

    assert traits.contains( Cascading.CONVENTION );
    }

  @Override
  public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs )
    {
    assert inputs.isEmpty();

    return new CascadingValuesRel( getCluster(), traitSet, rowType, tuples );
    }

  public Branch visitChild( Stack stack )
    {
    String pipeName = createUniqueName();
    Fields fields = RelUtil.createTypedFieldsFor( this, false );

    return new Branch( stack.heads, pipeName, fields, tuples );
    }
  }
