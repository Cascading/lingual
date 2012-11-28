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

package cascading.lingual.util;

/**
 *
 */
public class SimpleTypeMap extends TypeMap
  {
  public SimpleTypeMap()
    {
    put( "string", String.class );

    // primitives
    put( "boolean", Boolean.TYPE );
    put( "int", Integer.TYPE );
    put( "short", Short.TYPE );
    put( "long", Long.TYPE );
    put( "float", Float.TYPE );
    put( "byte", Byte.TYPE );

    // objects
    put( "Boolean", Boolean.class );
    put( "Integer", Integer.class );
    put( "Short", Short.class );
    put( "Long", Long.class );
    put( "Float", Float.class );
    put( "Byte", Byte.class );
    }
  }
