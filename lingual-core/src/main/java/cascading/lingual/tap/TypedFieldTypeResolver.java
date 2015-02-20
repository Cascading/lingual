/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import java.lang.reflect.Type;
import java.util.Map;

import cascading.lingual.type.TypeMap;
import cascading.scheme.util.FieldTypeResolver;

/**
 *
 */
public class TypedFieldTypeResolver implements FieldTypeResolver
  {
  private final TypeMap typeMap;
  private final Type defaultType;

  public TypedFieldTypeResolver( TypeMap typeMap, Type defaultType )
    {
    this.typeMap = typeMap;
    this.defaultType = defaultType;
    }

  @Override
  public Type inferTypeFrom( int ordinal, String fieldName )
    {
    for( Map.Entry<String, Type> entry : typeMap.getNameToTypeMap().entrySet() )
      {
      String pattern = entry.getKey();

      if( matches( pattern, fieldName ) )
        return entry.getValue();
      }

    return defaultType;
    }

  protected boolean matches( String pattern, String fieldName )
    {
    return fieldName.matches( String.format( ".*:%s$", pattern ) );
    }

  @Override
  public String prepareField( int i, String fieldName, Type type )
    {
    String pattern = typeMap.getNameFor( type );

    if( pattern == null )
      return fieldName;

    return String.format( "%s:%s", fieldName, pattern );
    }

  @Override
  public String cleanField( int ordinal, String fieldName, Type type )
    {
    return fieldName.replaceAll( ":.*$", "" );
    }
  }
