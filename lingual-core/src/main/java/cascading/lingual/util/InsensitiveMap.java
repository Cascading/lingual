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

import java.util.TreeMap;

/** Utility for force case-insensitivity, also eases json deserialization as there is a target class to instantiate */
public class InsensitiveMap<V> extends TreeMap<String, V>
  {
  public InsensitiveMap()
    {
    super( String.CASE_INSENSITIVE_ORDER );
    }

  @Override
  public V get( Object key )
    {
    return super.get( key.toString().toLowerCase() );
    }

  @Override
  public V put( String key, V value )
    {
    return super.put( key.toLowerCase(), value );
    }
  }
