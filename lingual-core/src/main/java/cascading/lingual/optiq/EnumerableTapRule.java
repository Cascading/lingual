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

import net.hydromatic.optiq.rules.java.EnumerableConvention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;

/**
 * Rule that converts a Cascading Tap into an enumerable.
 * <p/>
 * <p>Usually a Tap is the leaf of a plan that represents a Cascading flow,
 * and this results in a MapReduce job at runtime.
 * But if the query is just "select * from aTable" then it makes sense to read
 * directly from the tap.</p>
 */
public class EnumerableTapRule extends RelOptRule
  {
  public static final EnumerableTapRule INSTANCE = new EnumerableTapRule();

  private EnumerableTapRule()
    {
    super( leaf( CascadingTableAccessRel.class ), "EnumerableTapRule" );
    }

  @Override
  public void onMatch( RelOptRuleCall call )
    {
    final CascadingTableAccessRel tableRel = (CascadingTableAccessRel) call.getRels()[ 0 ];
    final TapEnumerableRel tapRel =
      new TapEnumerableRel(
        tableRel.getCluster(),
        tableRel.getTraitSet().plus( EnumerableConvention.ARRAY ),
        tableRel.getTable(),
        tableRel.name,
        tableRel.identifier );

    call.transformTo( tapRel );
    }
  }
