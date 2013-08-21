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

package cascading.lingual.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;

/** Class MultiProperties is a simple version of a nested multi-map, that is, a Map of MultiMap instances. */
public class MultiProperties<K> implements Serializable
  {
  @JsonIgnore
  private Table<K, String, List<String>> properties = HashBasedTable.create();

  public static <K> MultiProperties<K> create( Map<K, Map<String, List<String>>> map )
    {
    MultiProperties<K> multiProperties = new MultiProperties<K>();

    multiProperties.setProperties( map );

    return multiProperties;
    }

  public MultiProperties()
    {
    }

  @JsonSetter
  protected void setProperties( Map<K, Map<String, List<String>>> map )
    {
    for( Map.Entry<K, Map<String, List<String>>> entry : map.entrySet() )
      {
      K row = entry.getKey();
      Map<String, List<String>> value = entry.getValue();

      for( String column : value.keySet() )
        properties.put( row, column, value.get( column ) );
      }
    }

  @JsonGetter
  protected Map<K, Map<String, List<String>>> getProperties()
    {
    return properties.rowMap();
    }

  @JsonIgnore
  public Set<K> getKeys()
    {
    return properties.rowKeySet();
    }

  public void putProperties( K k, Map<String, List<String>> properties )
    {
    for( Map.Entry<String, List<String>> entry : properties.entrySet() )
      this.properties.put( k, entry.getKey(), entry.getValue() );
    }

  public void addProperty( K k, String property, String... values )
    {
    addProperty( k, property, asList( values ) );
    }

  public void addProperty( K k, String property, List<String> list )
    {
    if( !properties.row( k ).containsKey( property ) )
      properties.put( k, property, new ArrayList<String>() );

    Set<String> previous = newHashSet( properties.get( k, property ) );

    previous.addAll( list );

    properties.put( k, property, new ArrayList<String>( previous ) );
    }

  public Map<K, List<String>> getKeyFor( String property )
    {
    return properties.column( property );
    }

  public Map<String, List<String>> getValueFor( K k )
    {
    return properties.row( k );
    }

  public Map<String, List<String>> removeRow( K key )
    {
    if( !properties.containsRow( key ) )
      return null;

    Map<String, List<String>> returnValue = new HashMap<String, List<String>>();
    // two collections since can't iterate over keys while removing w/o getting ConcurrentModificationException.
    Map<String, List<String>> values = getValueFor( key );
    List<String> removedItems = new ArrayList<String>( values.size() );

    for( String columnName : values.keySet() )
      {
      returnValue.put( columnName, values.get( columnName ) );
      removedItems.add( columnName );
      }

    for( String columnName : removedItems )
      properties.remove( key, columnName );

    return returnValue.size() != 0 ? returnValue : null;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof MultiProperties ) )
      return false;

    MultiProperties that = (MultiProperties) object;

    if( properties != null ? !properties.equals( that.properties ) : that.properties != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    return properties != null ? properties.hashCode() : 0;
    }
  }
