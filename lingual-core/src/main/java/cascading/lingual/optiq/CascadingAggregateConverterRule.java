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

import org.eigenbase.rel.AggregateRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.CallingConvention;

import static cascading.lingual.optiq.CascadingCallingConvention.CASCADING;

/**
 *
 */
public class CascadingAggregateConverterRule extends ConverterRule
  {
  public static final CascadingAggregateConverterRule INSTANCE = new CascadingAggregateConverterRule();

  public CascadingAggregateConverterRule()
    {
    super( AggregateRel.class, CallingConvention.NONE, CASCADING, "CascadingAggregateRule" );
    }

  @Override
  public RelNode convert( RelNode rel )
    {
//    if( !rel.getTraitSet().contains( CallingConvention.NONE ) )
//      return null;

    // stolen from JavaRules.EnumerableAggregateRule

    AggregateRel agg = (AggregateRel) rel;

    RelNode convertedChild = mergeTraitsAndConvert( agg.getTraitSet(), CASCADING, agg.getChild() );

    if( convertedChild == null )
      return null; // We can't convert the child, so we can't convert rel.

//    if(agg.getAggCallList().isEmpty())
//      return new CascadingUnionRel( rel.getCluster(), rel.getTraitSet(), convertedChild)

    return new CascadingAggregateRel( rel.getCluster(), rel.getTraitSet(), convertedChild, agg.getGroupSet(), agg.getAggCallList() );
    }
  }
