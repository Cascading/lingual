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

package cascading.lingual.platform;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import cascading.bind.catalog.Resource;
import cascading.bind.catalog.Stereotype;
import cascading.bind.catalog.handler.ProtocolHandler;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.util.MultiProperties;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;

/**
 *
 */
public abstract class LingualProtocolHandler implements ProtocolHandler<Protocol, Format>, Serializable
  {
  private MultiProperties<Protocol> defaults = new MultiProperties<Protocol>();

  public MultiProperties<Protocol> getDefaults()
    {
    return defaults;
    }

  @Override
  public Map<String, List<String>> getDefaultProperties( Protocol protocol )
    {
    return getDefaults().getValueFor( protocol );
    }

  @Override
  public Tap createTap( Stereotype<Protocol, Format> stereotype, Resource<Protocol, Format, SinkMode> resource )
    {
    Scheme scheme = stereotype.getSchemeFor( resource.getProtocol(), resource.getFormat() );

    if( scheme == null )
      throw new IllegalStateException( "no scheme found for protocol: " + resource.getProtocol() + ", format: " + resource.getFormat() );

    Tap tap = createTapFor( resource, scheme );

    if( tap == null )
      throw new IllegalStateException( "no tap found for protocol: " + resource.getProtocol() + ", format: " + resource.getFormat() );

    return tap;
    }

  protected abstract Tap createTapFor( Resource<Protocol, Format, SinkMode> resource, Scheme scheme );
  }
