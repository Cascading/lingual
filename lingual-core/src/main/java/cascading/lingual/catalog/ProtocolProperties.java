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

package cascading.lingual.catalog;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ProtocolProperties extends SchemaProperties
  {
  public static final String URIS = "protocol.uris";

  public static Protocol findProtocolFor( SchemaDef schemaDef, String identifier )
    {
    if( schemaDef == null )
      return null;

    Map<Protocol, List<String>> uris = schemaDef.getProtocolProperties( URIS );

    URI uri = URI.create( identifier );

    for( Map.Entry<Protocol, List<String>> entry : uris.entrySet() )
      {
      if( entry.getValue().contains( uri.getScheme() ) )
        return entry.getKey();
      }

    return null;
    }
  }
