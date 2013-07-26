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
import java.util.List;
import java.util.Map;

import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.ProviderDef;

/**
 *
 */
public class ProviderBuilder extends Builder<ProviderDef>
  {
  public ProviderBuilder()
    {
    super( null );
    }

  @Override
  public Map format( ProviderDef providerDef )
    {
    Map providerMap = getDefProperties( providerDef );

    providerMap.put( "factory", providerDef.getFactoryClassName() );

    Map<Protocol, Map<String, List<String>>> protocolProperties = providerDef.getProtocolProperties();
    Map protocolsMap = new LinkedHashMap();

    protocolsMap.put( "names", protocolProperties.keySet() );
    protocolsMap.putAll( protocolProperties );

    providerMap.put( "protocols", protocolsMap );

    Map<Format, Map<String, List<String>>> formatProperties = providerDef.getFormatProperties();
    Map formatsMap = new LinkedHashMap();

    formatsMap.put( "names", formatProperties.keySet() );
    formatsMap.putAll( formatProperties );

    providerMap.put( "formats", formatsMap );

    return providerMap;
    }
  }
