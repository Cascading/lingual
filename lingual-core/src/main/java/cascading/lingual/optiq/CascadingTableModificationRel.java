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
import net.hydromatic.optiq.prepare.Prepare;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableModificationRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;

/**
 *
 */
public class CascadingTableModificationRel extends TableModificationRelBase implements CascadingRelNode
  {
  public CascadingTableModificationRel( RelOptCluster cluster, RelTraitSet traits, RelOptTable table,
                                        Prepare.CatalogReader catalogReader, RelNode child,
                                        Operation operation, List<String> updateColumnList, boolean flattened )
    {
    super( cluster, traits, table, catalogReader, child, operation, updateColumnList, flattened );
    }

  @Override
  public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs )
    {
    assert inputs.size() == 1;
    return new CascadingTableModificationRel(
      getCluster(),
      traitSet,
      getTable(),
      getCatalogReader(),
      inputs.get( 0 ),
      getOperation(),
      getUpdateColumnList(),
      isFlattened() );
    }

  @Override
  public void register( RelOptPlanner planner )
    {
    super.register( planner );

    // Most queries read from at least one cascading table, and the rules
    // get registered when the CascadingTableAccessRel is registered. We
    // register the rules here, for queries that only write to tables, such as
    //   INSERT INTO cascading_table VALUES ...;

    CascadingTableAccessRel.registerRules( planner );
    }

  @Override
  public Branch visitChild( Stack stack )
    {
    Branch branch = ( (CascadingRelNode) getChild() ).visitChild( stack );
    TableDef tableDef = getTapTable().getTableDef();

    return new Branch( getPlatformBroker(), branch, tableDef );
    }

  private TapTable getTapTable()
    {
    return getTable().unwrap( TapTable.class );
    }

  public PlatformBroker getPlatformBroker()
    {
    return getTapTable().getPlatformBroker();
    }
  }
