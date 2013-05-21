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

  static Fields createPermutationFields( Fields incomingFields, Permutation permutation )
    {
    Fields arguments = Fields.NONE;

    for( int i = 0; i < permutation.getTargetCount(); i++ )
      arguments = arguments.append( new Fields( incomingFields.get( permutation.getTarget( i ) ) ) );

    return arguments;
    }
  }
