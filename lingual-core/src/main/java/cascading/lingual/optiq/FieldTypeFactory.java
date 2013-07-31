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

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import cascading.tuple.Fields;
import net.hydromatic.optiq.jdbc.JavaTypeFactoryImpl;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.reltype.RelDataTypeFieldImpl;

/**
 *
 */
public class FieldTypeFactory extends JavaTypeFactoryImpl
  {
  public RelDataType createFieldsType( Fields sourceFields )
    {
    Type[] types = sourceFields.getTypes();
    List<RelDataTypeField> fields = new ArrayList<RelDataTypeField>();

    for( int i = 0; i < sourceFields.size(); i++ )
      {
      Type type = types != null && types[ i ] != null ? types[ i ] : String.class;

      RelDataType javaType;

      if( type instanceof Class )
        javaType = canonize( createJavaType( (Class) type ) );
      else if( type instanceof RelDataType )
        javaType = canonize( (RelDataType) type );
      else
        throw new IllegalStateException( "unknown type: " + type.getClass().getSimpleName() );

      Comparable comparable = sourceFields.get( i );
      String fieldName = comparable instanceof String ? (String) comparable : comparable.toString();

      fields.add( new RelDataTypeFieldImpl( fieldName, i, javaType ) );
      }

    return canonize( new FieldRecordType( sourceFields, fields ) );
    }
  }
