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

import java.lang.reflect.Type;

import cascading.bind.catalog.Schema;
import cascading.lingual.platform.PlatformBroker;
import cascading.tuple.Fields;
import net.hydromatic.linq4j.BaseQueryable;
import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.TranslatableTable;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;

/**
 *
 */
public class TapTable extends BaseQueryable implements TranslatableTable
  {
  private static FieldTypeFactory typeFactory = new FieldTypeFactory();

  private final MutableSchema parentTableSchema;
  private final PlatformBroker platformBroker;
  private final String identifier;
  private final Schema tapSchema;

  public TapTable( PlatformBroker platformBroker, QueryProvider provider, MutableSchema parentTableSchema, String identifier )
    {
    super( provider, null, null );
    this.platformBroker = platformBroker;
    this.parentTableSchema = parentTableSchema;
    this.identifier = identifier;
    this.tapSchema = platformBroker.getCatalog().getSchemaFor( identifier );
    }

  @Override
  public Type getElementType()
    {
    return typeFactory.createFieldsType( tapSchema.getFields() );
    }

  public String getName()
    {
    return tapSchema.getName();
    }

  public String getIdentifier()
    {
    return identifier;
    }

  public Fields getFields()
    {
    return tapSchema.getFields();
    }

  public DataContext getDataContext()
    {
    return parentTableSchema;
    }

  public RelNode toRel( RelOptTable.ToRelContext context, RelOptTable relOptTable )
    {
    return new CascadingTableAccessRel( context.getCluster(), relOptTable, platformBroker, getName(), identifier );
    }
  }
