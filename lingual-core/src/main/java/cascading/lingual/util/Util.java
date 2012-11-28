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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Interners;

/**
 *
 */
public class Util
  {
  public static <A, B> Map<B, A> invert( Map<A, B> map )
    {
    Map<B, A> result = new HashMap<B, A>();

    for( Map.Entry<A, B> entry : map.entrySet() )
      result.put( entry.getValue(), entry.getKey() );

    return result;
    }

  // todo: make file separator agnostic
  public static String createSchemaNameFrom( String identifier )
    {
    return URI.create( identifier ).getPath().replaceAll( "^.*/([^/]+)/?$", "$1" ).toUpperCase();
    }

  // todo: make file separator agnostic
  public static String createTableNameFrom( String identifier )
    {
    return URI.create( identifier ).getPath().replaceAll( "^.*/([^/.]+)\\..*$", "$1" ).toUpperCase();
    }

  public static <Key, Value> LoadingCache<Key, Value> makeInternedCache( Function<Key, Value> factory )
    {
    Function<Key, Value> interner = Functions.compose(
      Interners.asFunction( Interners.<Value>newStrongInterner() ),
      factory
    );

    return CacheBuilder.newBuilder().build( CacheLoader.from( interner ) );
    }
  }
