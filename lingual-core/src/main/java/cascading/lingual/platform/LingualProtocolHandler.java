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

package cascading.lingual.platform;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import cascading.bind.catalog.Resource;
import cascading.bind.catalog.handler.ProtocolHandler;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.ProviderDef;
import cascading.lingual.util.MultiProperties;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public abstract class LingualProtocolHandler implements ProtocolHandler<Protocol, Format>, Serializable
  {
  private final ProviderDef providerDef;
  private MultiProperties<Protocol> properties = new MultiProperties<Protocol>();

  protected final Logger LOG = LoggerFactory.getLogger( getClass() );

  protected LingualProtocolHandler( ProviderDef providerDef )
    {
    this.providerDef = providerDef;

    Map<Protocol, Map<String, List<String>>> protocols = providerDef.getProtocolProperties();

    for( Protocol protocol : protocols.keySet() )
      getProperties().putProperties( protocol, protocols.get( protocol ) );
    }

  public ProviderDef getProviderDef()
    {
    return providerDef;
    }

  /**
   * Wrap the resulting Tap in a proxy that swaps out the context classloader when the tap is used for reading
   * and writing
   */
  public abstract Tap createLoadableTap( Scheme scheme, Resource<Protocol, Format, SinkMode> resource );

  public void addProperties( Protocol protocol, Map<String, List<String>> values )
    {
    for( String key : values.keySet() )
      addProperty( protocol, key, values.get( key ) );
    }

  public void addProperty( Protocol protocol, String key, List<String> values )
    {
    if( values == null || values.isEmpty() )
      return;

    getProperties().addProperty( protocol, key, values );
    }

  public MultiProperties<Protocol> getProperties()
    {
    return properties;
    }

  @Override
  public Map<String, List<String>> getDefaultProperties( Protocol protocol )
    {
    return getProperties().getValueFor( protocol );
    }
  }
