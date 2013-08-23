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

import org.eigenbase.rel.SortRel;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

/**
 *
 */
class CascadingSortRule extends RelOptRule
  {
  public static final CascadingSortRule INSTANCE = new CascadingSortRule();

  private CascadingSortRule()
    {
    super( any( SortRel.class ), "CascadingSortRule" );
    }

  @Override
  public void onMatch( RelOptRuleCall call )
    {
    SortRel rel = call.rel( 0 );

    // Cascading sort does not implement limits
    if( rel.offset != null || rel.fetch != null )
      return;

    RelTraitSet newTraits = rel.getTraitSet().plus( Cascading.CONVENTION );

    call.transformTo( new CascadingSortRel(
      rel.getCluster(),
      newTraits,
      convert( rel.getChild(), newTraits ),
      rel.getCollation(),
      rel.offset,
      rel.fetch ) );
    }
  }
