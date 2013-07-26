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

import java.util.Map;

import cascading.bind.catalog.Stereotype;

/**
 *
 */
public class StereotypeBuilder extends Builder<Stereotype>
  {
  public StereotypeBuilder()
    {
    super( null );
    }

  @Override
  public Map format( Stereotype stereotype )
    {
    Map map = getMap();

    map.put( "formats", stereotype.getAllFormats() );
    map.put( "default format", stereotype.getDefaultFormat() );
    map.put( "protocols", stereotype.getAllProtocols() );
    map.put( "default protocol", stereotype.getDefaultProtocol() );
    map.put( "fields", stereotype.getFields() );

    return map;
    }
  }
