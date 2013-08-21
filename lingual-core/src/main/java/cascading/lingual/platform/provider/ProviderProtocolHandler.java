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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import cascading.bind.catalog.Resource;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.ProviderDef;
import cascading.lingual.catalog.provider.ProviderProxy;
import cascading.lingual.platform.LingualProtocolHandler;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.util.MiscCollection;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;

/**
 *
 */
public class ProviderProtocolHandler extends LingualProtocolHandler
  {
  private transient PlatformBroker platformBroker;
  private ProviderProxy providerProxy;

  public ProviderProtocolHandler( PlatformBroker platformBroker, ProviderDef providerDef )
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

  @Override
  public Collection<? extends Protocol> getProtocols()
    {
    return getProperties().getKeys();
    }

  @Override
  public boolean handles( Protocol protocol )
    {
    return getProtocols().contains( protocol );
    }

  @Override
  public Tap createTap( Scheme scheme, Resource<Protocol, Format, SinkMode> resource )
    {
    Map<String, List<String>> defaultProperties = getDefaultProperties( resource.getProtocol() );
    Properties properties = MiscCollection.asProperties( defaultProperties );

    return getProviderProxy().createTap( resource, scheme, properties );
    }

  @Override
  public Tap createLoadableTap( Scheme scheme, Resource<Protocol, Format, SinkMode> resource )
    {
    return getProviderProxy().createTapProxy( createTap( scheme, resource ) );
    }
  }
