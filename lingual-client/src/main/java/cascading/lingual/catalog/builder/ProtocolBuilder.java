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

import java.util.List;
import java.util.Map;

import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.SchemaDef;

/**
 *
 */
public class ProtocolBuilder extends Builder<Protocol>
  {
  private final String providerName;

  public ProtocolBuilder( SchemaDef schemaDef, String providerName )
    {
    super( schemaDef );
    this.providerName = providerName;
    }

  @Override
  public Map format( Protocol protocol )
    {
    Map map = getMap();

    try
      {
      map.put( protocol, schemaDef.findAllProtocolProperties( protocol ) );
      }
    catch( IllegalStateException exception )
      {
      Map<String, Map<String, List<String>>> providerProperties = schemaDef.findProviderProtocolProperties( protocol );

      if( providerProperties.keySet().size() == 1 )
        map.put( protocol, providerProperties.values().iterator().next() );
      else if( providerProperties.containsKey( providerName ) )
        map.put( protocol, providerProperties.get( providerName ) );
      else if( providerName == null )
        throw new IllegalStateException( exception.getMessage() + ", use --provider to specify" );
      else
        throw new IllegalStateException( "provider: " + providerName + ", not found" );
      }

    return map;
    }
  }
