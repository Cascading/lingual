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

package cascading.lingual.platform.provider;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.bind.catalog.Stereotype;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.ProviderDef;
import cascading.lingual.catalog.provider.ProviderProxy;
import cascading.lingual.platform.LingualFormatHandler;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.util.MultiProperties;
import cascading.scheme.Scheme;

import static cascading.lingual.catalog.Protocol.resolveProtocols;
import static cascading.lingual.util.MiscCollection.asProperties;
import static com.google.common.collect.Lists.newCopyOnWriteArrayList;

/**
 *
 */
public class ProviderFormatHandler extends LingualFormatHandler
  {
  private transient PlatformBroker platformBroker;
  private ProviderProxy providerProxy;

  public ProviderFormatHandler( PlatformBroker platformBroker, ProviderDef providerDef )
    {
    super( providerDef );
    this.platformBroker = platformBroker;
    }

  private ProviderProxy getProviderProxy()
    {
    if( providerProxy == null )
      providerProxy = new ProviderProxy( platformBroker, getProviderDef() );

    return providerProxy;
    }

  public Map<Format, List<Protocol>> getFormatProtocols()
    {
    Set<Protocol> defaultProtocols = getProviderDef().getProtocolProperties().keySet();
    Map<Format, List<Protocol>> result = new HashMap<Format, List<Protocol>>();
    MultiProperties<Format> formatProperties = getProperties();

    for( Format format : formatProperties.getKeys() )
      {
      Map<String, List<String>> properties = formatProperties.getValueFor( format );
      List<Protocol> protocols = resolveProtocols( properties.get( "protocols" ) );

      // if not protocols are specified, use those declared in the provider.properties
      if( protocols.isEmpty() )
        result.put( format, newCopyOnWriteArrayList( defaultProtocols ) );
      else
        result.put( format, protocols );
      }

    return result;
    }

  @Override
  public Collection<? extends Format> getFormats()
    {
    return getProperties().getKeys();
    }

  @Override
  public boolean handles( Protocol protocol, Format format )
    {
    Map<Format, List<Protocol>> formatProtocols = getFormatProtocols();

    if( !formatProtocols.containsKey( format ) )
      return false;

    return formatProtocols.get( format ).contains( protocol );
    }

  @Override
  public Scheme createScheme( Stereotype<Protocol, Format> stereotype, Protocol protocol, Format format )
    {
    Map<String, List<String>> defaultProperties = getDefaultProperties( format );
    Properties properties = asProperties( defaultProperties );

    return getProviderProxy().createScheme( stereotype, protocol, format, properties );
    }

  @Override
  public Scheme createLoadableScheme( Stereotype<Protocol, Format> stereotype, Protocol protocol, Format format )
    {
    return getProviderProxy().createSchemeProxy( createScheme( stereotype, protocol, format ) );
    }
  }
