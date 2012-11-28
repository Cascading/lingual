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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

/**
 *
 */
public class MultiProperties<K> implements Serializable
  {
  private Table<K, String, List<String>> properties = HashBasedTable.create();

  public MultiProperties()
    {
    }

  public void addProperties( K k, Map<String, List<String>> properties )
    {
    for( Map.Entry<String, List<String>> entry : properties.entrySet() )
      this.properties.put( k, entry.getKey(), entry.getValue() );
    }

  public void addProperty( K k, String property, String... values )
    {
    addProperty( k, property, Arrays.asList( values ) );
    }

  public void addProperty( K k, String property, List<String> list )
    {
    if( !properties.row( k ).containsKey( property ) )
      properties.put( k, property, new ArrayList<String>() );

    properties.get( k, property ).addAll( list );
    }

  public Map<K, List<String>> getKeyFor( String property )
    {
    return properties.column( property );
    }

  public Map<String, List<String>> getValueFor( K k )
    {
    return properties.row( k );
    }
  }
