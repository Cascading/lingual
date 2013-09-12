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
import org.eigenbase.util.Permutation;

/**
 *
 */
class RelUtil
  {
  public static Fields createTypedFieldsFor( RelOptCluster cluster, List<Integer> keys, RelDataType rowType, boolean numeric )
    {
    Fields fields = Fields.NONE;

    for( Integer key : keys )
      fields = fields.append( createTypedFieldsFor( cluster, rowType.getFieldList().get( key ), numeric ) );

    return fields;
    }

  public static Fields createTypedFieldsSelectorFor( RelOptCluster cluster, List<Integer> keys, RelDataType rowType, boolean numeric )
    {
    Fields fields = Fields.NONE;

    for( Integer key : keys )
      fields = fields.appendSelector( createTypedFieldsFor( cluster, rowType.getFieldList().get( key ), numeric ) );

    return fields;
    }

  public static Fields createTypedFieldsFor( RelNode relNode, boolean numeric )
    {
    RelDataType rowType = relNode.getRowType();
    RelOptCluster cluster = relNode.getCluster();

    return createTypedFields( cluster, rowType, numeric );
    }

  public static Fields createTypedFieldsSelectorFor( RelNode relNode, boolean numeric )
    {
    RelDataType rowType = relNode.getRowType();
    RelOptCluster cluster = relNode.getCluster();

    return createTypedFieldsSelector( cluster, rowType, numeric );
    }

  public static Fields createTypedFields( RelOptCluster cluster, RelDataType rowType, boolean numeric )
    {
    return createTypedFields( cluster, rowType.getFieldList(), numeric );
    }

  public static Fields createTypedFieldsSelector( RelOptCluster cluster, RelDataType rowType, boolean numeric )
    {
    return createTypedFieldsSelector( cluster, rowType.getFieldList(), numeric );
    }

  public static Fields createTypedFields( RelOptCluster cluster, List<RelDataTypeField> typeFields, boolean numeric )
    {
    Fields fields = Fields.NONE;

    for( RelDataTypeField typeField : typeFields )
      fields = fields.append( createTypedFieldsFor( cluster, typeField, numeric ) );

    return fields;
    }

  public static Fields createTypedFieldsSelector( RelOptCluster cluster, List<RelDataTypeField> typeFields, boolean numeric )
    {
    Fields fields = Fields.NONE;

    for( RelDataTypeField typeField : typeFields )
      fields = fields.appendSelector( createTypedFieldsFor( cluster, typeField, numeric ) );

    return fields;
    }

  // Using numeric fields is more robust if we're not sure of input row types
  // but more difficult for humans to debug.
  public static Fields createTypedFieldsFor( RelOptCluster cluster, RelDataTypeField typeField, boolean numeric )
    {
    Class type = getJavaType( cluster, typeField.getType() );

    if( numeric )
      return new Fields( typeField.getIndex(), type );
    else
      return new Fields( typeField.getName(), type );
    }

  public static Fields createTypedFields( RelOptCluster cluster, RelDataType rowType, Iterable<Integer> fieldList, boolean numeric )
    {
    List<Fields> fields = new ArrayList<Fields>();

    for( Integer index : fieldList )
      {
      RelDataTypeField relDataTypeField = rowType.getFieldList().get( index );

      String fieldName = relDataTypeField.getName();
      Class fieldType = getJavaType( cluster, relDataTypeField.getType() );

      if( !numeric )
        fields.add( new Fields( fieldName, fieldType ) );
      else
        fields.add( new Fields( index, fieldType ) );
      }

    // hides duplicate names
    return Fields.join( fields.toArray( new Fields[ fields.size() ] ) );
    }

  public static Class getJavaType( RelOptCluster cluster, RelDataType dataType )
    {
    return (Class) ( (JavaTypeFactory) cluster.getTypeFactory() ).getJavaClass( dataType );
    }

  static Fields createPermutationFields( Fields incomingFields, Permutation permutation )
    {
    Fields arguments = Fields.NONE;

    for( int i = 0; i < permutation.getTargetCount(); i++ )
      arguments = arguments.append( new Fields( incomingFields.get( permutation.getTarget( i ) ) ) );

    return arguments;
    }
  }
