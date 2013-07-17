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

import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexLiteral;

import static cascading.lingual.optiq.Cascading.CONVENTION;

/**
 *
 */
class CascadingInsertValuesRule extends RelOptRule
  {
  public static final CascadingInsertValuesRule INSTANCE = new CascadingInsertValuesRule();

  private CascadingInsertValuesRule()
    {
    super(
      some( CascadingTableModificationRel.class, CONVENTION,
        some( CascadingCalcRel.class, CONVENTION,
          leaf( CascadingValuesRel.class, CONVENTION
          )
        )
      ),
      "CascadingInsertValuesRule" );
    }

  @Override
  public void onMatch( RelOptRuleCall call )
    {
    CascadingTableModificationRel modificationRel = call.rel( 0 );
    CascadingValuesRel valuesRel = call.rel( 2 );

    RelTraitSet newTraits = modificationRel.getTraitSet().plus( CONVENTION );
    RelOptCluster cluster = modificationRel.getCluster();

    RelDataType rowType = modificationRel.getRowType();

    RelOptTable table = modificationRel.getTable();
    List<List<RexLiteral>> tuples = valuesRel.getTuples();

    call.transformTo(
      new CascadingInsertValuesRel(
        cluster,
        newTraits,
        rowType,
        table,
        tuples
      ) );
    }
  }
