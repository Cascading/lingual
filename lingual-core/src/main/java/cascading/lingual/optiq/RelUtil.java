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

import java.util.ArrayList;
import java.util.List;

import cascading.tuple.Fields;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexSlot;

/**
 *
 */
public class RelUtil
  {
  public static Fields createTypedFieldsFor( RelOptCluster cluster, List<Integer> keys, RelDataType rowType )
    {
    Fields fields = Fields.NONE;

    for( Integer key : keys )
      fields = fields.append( createTypedFieldsFor( cluster, rowType.getFieldList().get( key ) ) );

    return fields;
    }

  public static Fields createTypedFieldsFor( RelNode relNode )
    {
    RelDataType rowType = relNode.getRowType();
    RelOptCluster cluster = relNode.getCluster();

    return createTypedFields( cluster, rowType );
    }

  public static Fields createTypedFields( RelOptCluster cluster, RelDataType rowType )
    {
    Fields fields = Fields.NONE;

    for( RelDataTypeField typeField : rowType.getFields() )
      fields = fields.append( createTypedFieldsFor( cluster, typeField ) );

    return fields;
    }

  public static Fields createTypedFieldsFor( RelOptCluster cluster, RelDataTypeField typeField )
    {
    String name = typeField.getName();
    Class type = getJavaType( cluster, typeField.getType() );

    return new Fields( name, type );
    }

  public static Fields createTypedFields( RelOptCluster cluster, RelDataType rowType, Iterable<Integer> fieldList )
    {
    List<Fields> fields = new ArrayList<Fields>();

    for( Integer index : fieldList )
      {
      RelDataTypeField relDataTypeField = rowType.getFieldList().get( index );

      String fieldName = relDataTypeField.getName();
      Class fieldType = getJavaType( cluster, relDataTypeField.getType() );

      fields.add( new Fields( fieldName, fieldType ) );
      }

    // hides duplicate names
    return Fields.join( fields.toArray( new Fields[ fields.size() ] ) );
    }

  public static Class getJavaType( RelOptCluster cluster, RelDataType dataType )
    {
    return (Class) ( (JavaTypeFactory) cluster.getTypeFactory() ).getJavaClass( dataType );
    }

  static Fields createTypedFields( RelNode relNode, List<? extends RexNode> rexNodes )
    {
    return createTypedFields( relNode, rexNodes.toArray( new RexNode[ rexNodes.size() ] ) );
    }

  static Fields createTypedFields( RelNode relNode, RexNode[] rexNodes )
    {
    RelOptCluster cluster = relNode.getCluster();
    RelDataType inputRowType = relNode.getRowType();

    return createTypedFields( cluster, inputRowType, rexNodes );
    }

  public static Fields createTypedFields( RelOptCluster cluster, RelDataType inputRowType, List<? extends RexNode> rexNodes )
    {
    return createTypedFields( cluster, inputRowType, rexNodes.toArray( new RexNode[ rexNodes.size() ] ) );
    }

  public static Fields createTypedFields( RelOptCluster cluster, RelDataType inputRowType, RexNode[] rexNodes )
    {
    Fields fields = Fields.NONE;
    List<RelDataTypeField> fieldList = inputRowType.getFieldList();

    for( RexNode exp : rexNodes )
      {
      if( !( exp instanceof RexSlot ) )
        continue;

      RelDataTypeField dataTypeField = fieldList.get( ( (RexSlot) exp ).getIndex() );
      String name = dataTypeField.getName();
      RelDataType dataType = dataTypeField.getType();
      Class type = getJavaType( cluster, dataType );
      fields = fields.append( new Fields( name, type ) );
      }

    return fields;
    }
  }
