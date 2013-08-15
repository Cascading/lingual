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

import cascading.bind.catalog.Stereotype;
import cascading.bind.catalog.handler.FormatHandler;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.ProviderDef;
import cascading.lingual.util.MultiProperties;
import cascading.scheme.Scheme;

/**
 *
 */
public abstract class LingualFormatHandler implements FormatHandler<Protocol, Format>, Serializable
  {
  protected final ProviderDef providerDef;
  private MultiProperties<Format> properties = new MultiProperties<Format>();

  public LingualFormatHandler( ProviderDef providerDef )
    {
    this.providerDef = providerDef;

    Map<Format, Map<String, List<String>>> formats = providerDef.getFormatProperties();

    for( Format format : formats.keySet() )
      getProperties().putProperties( format, formats.get( format ) );
    }

  public ProviderDef getProviderDef()
    {
    return providerDef;
    }

  /**
   * Wrap the resulting Scheme in a proxy that swaps out the context classloader.
   * <p/>
   * Currently unused, but here for completeness
   */
  public abstract Scheme createLoadableScheme( Stereotype<Protocol, Format> stereotype, Protocol protocol, Format format );

  public void addProperties( Format format, Map<String, List<String>> values )
    {
    for( String key : values.keySet() )
      addProperty( format, key, values.get( key ) );
    }

  public void addProperty( Format format, String key, List<String> values )
    {
    if( values == null || values.isEmpty() )
      return;

    getProperties().addProperty( format, key, values );
    }

  protected MultiProperties<Format> getProperties()
    {
    return properties;
    }

  @Override
  public Map<String, List<String>> getDefaultProperties( Format format )
    {
    return getProperties().getValueFor( format );
    }
  }
