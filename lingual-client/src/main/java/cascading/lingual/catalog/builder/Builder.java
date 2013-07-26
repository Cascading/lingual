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

package cascading.lingual.catalog.builder;

import java.util.LinkedHashMap;
import java.util.Map;

import cascading.lingual.catalog.Def;
import cascading.lingual.catalog.SchemaDef;

/**
 *
 */
public abstract class Builder<T>
  {
  protected final SchemaDef schemaDef;

  public Builder( SchemaDef schemaDef )
    {
    this.schemaDef = schemaDef;
    }

  public abstract Map format( T def );

  protected Map getDefProperties( Def def )
    {
    Map map = getMap();

    map.put( "name", def.getName() );

    if( !( def instanceof SchemaDef ) )
      map.put( "schema", def.getParentSchema().getName() );

    map.put( "identifier", def.getIdentifier() );

    return map;
    }

  protected Map getMap()
    {
    return new LinkedHashMap();
    }
  }
