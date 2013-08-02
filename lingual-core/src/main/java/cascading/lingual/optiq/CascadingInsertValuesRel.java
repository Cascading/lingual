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

import cascading.lingual.catalog.TableDef;
import cascading.lingual.optiq.meta.Branch;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.tap.TapTable;
import org.eigenbase.rel.AbstractRelNode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexLiteral;

/**
 *
 */
class CascadingInsertValuesRel extends AbstractRelNode implements CascadingRelNode
  {
  private final RelOptTable table;
  private final List<List<RexLiteral>> tuples;

  public CascadingInsertValuesRel( RelOptCluster cluster, RelTraitSet traits, RelDataType rowType, RelOptTable table, List<List<RexLiteral>> tuples )
    {
    super( cluster, traits );
    this.rowType = rowType;
    this.table = table;
    this.tuples = tuples;
    }

  @Override
  public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs )
    {
    return new CascadingInsertValuesRel(
      getCluster(),
      traitSet,
      rowType, table,
      tuples
    );
    }

  @Override
  public RelOptTable getTable()
    {
    return table;
    }

  public List<List<RexLiteral>> getTuples()
    {
    return tuples;
    }

  @Override
  public Branch visitChild( Stack stack )
    {
    PlatformBroker platformBroker = getPlatformBroker();
    TableDef tableDef = getTapTable().getTableDef();

    return new Branch( platformBroker, tableDef, getTuples() );
    }

  public PlatformBroker getPlatformBroker()
    {
    return getTapTable().getPlatformBroker();
    }

  private TapTable getTapTable()
    {
    return getTable().unwrap( TapTable.class );
    }
  }
