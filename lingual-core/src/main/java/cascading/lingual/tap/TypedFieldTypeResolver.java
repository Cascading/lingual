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

package cascading.lingual.tap;

import java.util.HashMap;
import java.util.Map;

import cascading.lingual.util.Util;
import cascading.scheme.util.RegexFieldTypeResolver;

/**
 *
 */
public class TypedFieldTypeResolver extends RegexFieldTypeResolver
  {
  static final Map<String, Class> fieldTypeMap;
  static final Map<Class, String> typeFieldMap;

  static
    {
    fieldTypeMap = new HashMap<String, Class>();

    fieldTypeMap.put( ":string", String.class );

    // primitives
    fieldTypeMap.put( ":boolean", Boolean.TYPE );
    fieldTypeMap.put( ":int", Integer.TYPE );
    fieldTypeMap.put( ":short", Short.TYPE );
    fieldTypeMap.put( ":long", Long.TYPE );
    fieldTypeMap.put( ":float", Float.TYPE );
    fieldTypeMap.put( ":byte", Byte.TYPE );

    // objects
    fieldTypeMap.put( ":Boolean", Boolean.class );
    fieldTypeMap.put( ":Integer", Integer.class );
    fieldTypeMap.put( ":Short", Short.class );
    fieldTypeMap.put( ":Long", Long.class );
    fieldTypeMap.put( ":Float", Float.class );
    fieldTypeMap.put( ":Byte", Byte.class );

    typeFieldMap = Util.invert( fieldTypeMap );
    }

  public TypedFieldTypeResolver()
    {
    super( fieldTypeMap, String.class );
    }

  @Override
  protected boolean matches( String pattern, String fieldName )
    {
    return super.matches( ".*" + pattern + "$", fieldName );
    }

  @Override
  public String prepareField( int i, String fieldName, Class type )
    {
    String pattern = typeFieldMap.get( type );

    if( pattern == null )
      return fieldName;

    return fieldName + pattern;
    }

  @Override
  public String cleanField( int ordinal, String fieldName, Class type )
    {
    return fieldName.replaceAll( ":.*$", "" );
    }
  }
