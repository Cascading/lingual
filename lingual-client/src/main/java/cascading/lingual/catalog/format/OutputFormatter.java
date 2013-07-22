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

package cascading.lingual.catalog.format;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import cascading.lingual.catalog.Def;
import cascading.lingual.catalog.SchemaDef;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 *
 */
public abstract class OutputFormatter<T>
  {
  protected final SchemaDef schemaDef;

  public OutputFormatter( SchemaDef schemaDef )
    {
    this.schemaDef = schemaDef;
    }

  public abstract Collection<String> format( T def );

  protected Collection<String> toStringCollection( Map map )
    {
    Function<Map.Entry, String> kayValueCombiner =
      new Function<Map.Entry, String>()
      {
      public String apply( Map.Entry entry )
        {
        return String.valueOf( entry.getKey() ) + ": " + String.valueOf( entry.getValue() );
        }
      };

    return Collections2.transform( map.entrySet(), kayValueCombiner );
    }

  protected Map getDefProperties( Def def )
    {
    Map map = new HashMap();
    map.put( "identifier", def.getIdentifier() );
    map.put( "parent schema", def.getParentSchema().getName() );
    return map;
    }
  }
