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

import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.rex.RexMultisetUtil;

/**
 *
 */
public class CascadingCalcConverterRule extends ConverterRule
  {
  public static final CascadingCalcConverterRule INSTANCE = new CascadingCalcConverterRule();

  public CascadingCalcConverterRule()
    {
    super( CalcRel.class, CallingConvention.NONE, Cascading.CONVENTION, "CascadingCalcRule" );
    }

  @Override
  public RelNode convert( RelNode rel )
    {
    // stolen from JavaRules.EnumerableAggregateRule
    final CalcRel calc = (CalcRel) rel;

    final RelNode convertedChild =
      mergeTraitsAndConvert( calc.getTraitSet(), Cascading.CONVENTION, calc.getChild() );

    if( convertedChild == null )
      {
      return null; // We can't convert the child, so we can't convert rel.
      }

    // If there's a multiset, let FarragoMultisetSplitter work on it
    // first.
    if( RexMultisetUtil.containsMultiset( calc.getProgram() ) )
      {
      return null;
      }

    return new CascadingCalcRel( rel.getCluster(), rel.getTraitSet(), convertedChild, rel.getRowType(), calc.getProgram(), Collections.<RelCollation>emptyList() );
    }
  }
