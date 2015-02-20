/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

/**
 *
 */
class CascadingFilterRel extends FilterRelBase implements CascadingRelNode
  {
  public CascadingFilterRel( RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition )
    {
    super( cluster, traits, child, condition );

    assert child.getTraitSet().contains( Cascading.CONVENTION );
    }

  @Override
  public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs )
    {
    return new CascadingFilterRel(
      getCluster(),
      traitSet,
      sole( inputs ),
      getCondition() );
    }

  public Branch visitChild( Stack stack )
    {
    throw new UnsupportedOperationException( "unimplemented" );
//    Branch branch = ( (CascadingRelNode) getChild() ).visitChild( stack );
//
//    return null;
    }
  }
