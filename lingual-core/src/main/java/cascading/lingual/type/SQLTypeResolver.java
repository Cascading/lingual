/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

import cascading.lingual.tap.TypedFieldTypeResolver;

/**
 * SQLTypeResolver is a SQL type specific implementation of {@link TypedFieldTypeResolver}. It allows for text based
 * files to embed their type information along with column (field) name information.
 * <p/>
 * Specifically it uses the {@link SQLTypeMap} to resolve type names to their corresponding SQL type in the JVM.
 * <p/>
 * When used with a {@link cascading.scheme.Scheme} implementation supporting a {@link cascading.scheme.util.DelimitedParser}
 * like any of the {@code TextDelimited} implementations, it allows for headers like the following:
 * <p/>
 * {@code EMPNO:int,ITEM:string,SALE_DATE:date,SALE_TIME:time}
 * <p/>
 * During runtime this header will become the source {@link cascading.tuple.Fields} for a given
 * {@link cascading.tap.Tap}. The equivalent of:
 * <p/>
 * {@code new Fields( "EMPNO", Integer.TYPE ).append( "ITEM", String.class ).append( "SALE_DATE", new SQLDateCoercibleType() ).append( "SALE_TIME", new SQLTimeCoercibleType() );}
 * <p/>
 * This resolver converts the following names to types:
 * <p/>
 * <ul>
 * <li>string -> {@link String}</li>
 * <p/>
 * <li>byte -> {@link Byte#TYPE}</li>
 * <li>boolean -> {@link Boolean#TYPE}</li>
 * <li>short -> {@link Short#TYPE}</li>
 * <li>int -> {@link Integer#TYPE}</li>
 * <li>long -> {@link Long#TYPE}</li>
 * <li>float -> {@link Float#TYPE}</li>
 * <li>double -> {@link Double#TYPE}</li>
 * <p/>
 * <li>Boolean -> {@link Boolean}</li>
 * <li>Byte -> {@link Byte}</li>
 * <li>Short -> {@link Short}</li>
 * <li>Integer -> {@link Integer}</li>
 * <li>Long -> {@link Long}</li>
 * <li>Float -> {@link Float}</li>
 * <li>Double -> {@link Double}</li>
 * <p/>
 * <li>date -> {@link SQLDateCoercibleType}</li>
 * <li>time -> {@link SQLTimeCoercibleType}</li>
 * <li>timestamp -> {@link SQLTimestampCoercibleType}</li>
 * </ul>
 * <p/>
 * Note that all primitive types cannot be represented as {@code Null}, to make a field (column) nullable, use
 * an Object type. For example, instead of declaring {@code int}, use {@code Integer}.
 * <p/>
 * The default type if none is provided will be {@code string}.
 */
public class SQLTypeResolver extends TypedFieldTypeResolver
  {
  public SQLTypeResolver()
    {
    super( new SQLTypeMap(), String.class );
    }
  }
