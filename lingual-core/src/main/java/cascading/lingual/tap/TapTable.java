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

package cascading.lingual.tap;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import cascading.lingual.catalog.TableDef;
import cascading.lingual.optiq.Cascading;
import cascading.lingual.optiq.CascadingTableAccessRel;
import cascading.lingual.optiq.CascadingTableModificationRel;
import cascading.lingual.optiq.FieldTypeFactory;
import cascading.lingual.platform.PlatformBroker;
import cascading.tuple.Fields;
import net.hydromatic.linq4j.BaseQueryable;
import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.ModifiableTable;
import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;
import net.hydromatic.optiq.TranslatableTable;
import net.hydromatic.optiq.prepare.Prepare;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableModificationRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;

/** Class TapTable is an implementation of the Optiq {@link ModifiableTable} interface. */
public class TapTable extends BaseQueryable implements TranslatableTable, ModifiableTable
  {
  private static FieldTypeFactory typeFactory = new FieldTypeFactory();

  private final PlatformBroker platformBroker;
  private final MutableSchema parentTableSchema;
  private final TableDef tableDef;
  private final boolean useFullName;
  private final RelDataType rowType;

  public TapTable( PlatformBroker platformBroker, QueryProvider provider, TapSchema parentSchema, TableDef tableDef )
    {
    this( platformBroker, provider, parentSchema, tableDef, false );
    }

  public TapTable( PlatformBroker platformBroker, QueryProvider provider, TapSchema parentSchema, TableDef tableDef, boolean useFullName )
    {
    super( provider, Object.class, null );

    this.platformBroker = platformBroker;
    this.parentTableSchema = parentSchema;
    this.tableDef = tableDef;
    this.useFullName = useFullName;
    this.rowType = typeFactory.createFieldsType( getFields() );
    }

  public PlatformBroker getPlatformBroker()
    {
    return platformBroker;
    }

  public boolean isUseFullName()
    {
    return useFullName;
    }

  @Override
  public RelDataType getRowType()
    {
    return rowType;
    }

  public String getName()
    {
    return tableDef.getName();
    }

  public TableDef getTableDef()
    {
    return tableDef;
    }

  public String getIdentifier()
    {
    return tableDef.getIdentifier();
    }

  public Fields getFields()
    {
    return tableDef.getFields();
    }

  public DataContext getDataContext()
    {
    return parentTableSchema;
    }

  public Statistic getStatistic()
    {
    return Statistics.UNKNOWN;
    }

  public RelNode toRel( RelOptTable.ToRelContext context, RelOptTable table )
    {
    RelOptCluster cluster = context.getCluster();

    return new CascadingTableAccessRel( cluster, table, getName(), getIdentifier() );
    }

  public Collection getModifiableCollection()
    {
    return Collections.emptyList();
    }

  public TableModificationRelBase toModificationRel( RelOptCluster cluster,
                                                     RelOptTable table,
                                                     Prepare.CatalogReader catalogReader,
                                                     RelNode input,
                                                     TableModificationRelBase.Operation operation,
                                                     List updateColumnList,
                                                     boolean flattened )
    {
    RelTraitSet traits = input.getTraitSet().replace( Cascading.CONVENTION );
    RelNode convertedInput = RelOptRule.convert( input, traits );

    return new CascadingTableModificationRel( cluster, traits, table, catalogReader,
      convertedInput, operation, updateColumnList, flattened );
    }
  }
