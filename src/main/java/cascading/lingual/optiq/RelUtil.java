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

import java.util.ArrayList;
import java.util.List;

import cascading.tuple.Fields;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;

/**
 *
 */
public class RelUtil
  {
  public static Fields getFieldsFor( List<Integer> keys, RelDataType rowType )
    {
    Fields fields = new Fields();

    for( Integer key : keys )
      fields = fields.append( new Fields( rowType.getFieldList().get( key ).getName() ) );

    return fields;
    }

  public static Fields getFieldsFor( RelNode relNode )
    {
    RelDataTypeField[] typeFields = relNode.getRowType().getFields();
    Fields fields = new Fields();

    for( RelDataTypeField typeField : typeFields )
      fields = fields.append( new Fields( typeField.getName() ) );

    return fields;
    }

  public static Fields getTypedFieldsFor( RelNode relNode )
    {
    RelDataType rowType = relNode.getRowType();
    RelOptCluster cluster = relNode.getCluster();

    return getTypedFields( cluster, rowType );
    }

  public static Fields getTypedFields( RelOptCluster cluster, RelDataType rowType )
    {
    RelDataTypeField[] typeFields = rowType.getFields();
    Fields fields = new Fields();

    for( RelDataTypeField typeField : typeFields )
      {
      String name = typeField.getName();
      Class type = getJavaType( cluster, typeField.getType() );

      fields = fields.append( new Fields( name, type ) );
      }
    return fields;
    }

  public static Fields getTypedFields( RelOptCluster cluster, RelDataType inputRowType, Iterable<Integer> fieldList )
    {
    List<Fields> argFields = new ArrayList<Fields>();

    for( Integer index : fieldList )
      {
      RelDataTypeField relDataTypeField = inputRowType.getFieldList().get( index );

      String argName = relDataTypeField.getName();
      Class argType = getJavaType( cluster, relDataTypeField.getType() );

      argFields.add( new Fields( argName, argType ) );
      }

    // hides duplicate names
    return Fields.join( argFields.toArray( new Fields[ argFields.size() ] ) );
    }

  public static Class getJavaType( RelOptCluster cluster, RelDataType dataType )
    {
    return (Class) ( (JavaTypeFactory) cluster.getTypeFactory() ).getJavaClass( dataType );
    }

  static Fields createFields( RelNode relNode, List<RexNode> rexNodes )
    {
    return createFields( relNode, rexNodes.toArray( new RexNode[ rexNodes.size() ] ) );
    }

  static Fields createFields( RelNode relNode, RexNode[] rexNodes )
    {
    Fields fields = new Fields();
    RelDataType inputRowType = relNode.getRowType();
    List<RelDataTypeField> fieldList = inputRowType.getFieldList();

    for( RexNode exp : rexNodes )
      {
      if( !( exp instanceof RexInputRef ) )
        continue;

      RelDataTypeField dataTypeField = fieldList.get( ( (RexInputRef) exp ).getIndex() );
      String name = dataTypeField.getName();
      RelDataType dataType = dataTypeField.getType();
      Class type = getJavaType( relNode.getCluster(), dataType );
      fields = fields.append( new Fields( name, type ) );
      }

    return fields;
    }
  }
