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

package cascading.lingual.type;

/**
 * SQLTypeMap is an implementation of {@link TypeMap} for resolving string based type information
 * into its corresponding Java SQL type.
 * <p/>
 * SQLTypeMap is used by the {@link SQLTypeResolver}.
 */
public class SQLTypeMap extends TypeMap
  {
  public SQLTypeMap()
    {
    put( "string", String.class );

    // primitives
    put( "boolean", Boolean.TYPE );
    put( "byte", Byte.TYPE );
    put( "short", Short.TYPE );
    put( "int", Integer.TYPE );
    put( "long", Long.TYPE );
    put( "float", Float.TYPE );
    put( "double", Double.TYPE );

    // objects
    put( "Byte", Byte.class );
    put( "Boolean", Boolean.class );
    put( "Short", Short.class );
    put( "Integer", Integer.class );
    put( "Long", Long.class );
    put( "Float", Float.class );
    put( "Double", Double.class );

    put( "date", new SQLDateCoercibleType() );
    put( "time", new SQLTimeCoercibleType() );
    put( "timestamp", new SQLTimestampCoercibleType() );
    }
  }
