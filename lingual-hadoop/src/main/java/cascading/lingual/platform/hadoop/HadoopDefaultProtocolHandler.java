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

package cascading.lingual.platform.hadoop;

import java.util.Collection;
import java.util.Collections;

import cascading.bind.catalog.Resource;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.platform.LingualProtocolHandler;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

/**
 *
 */
public class HadoopDefaultProtocolHandler extends LingualProtocolHandler
  {
  public static final Protocol HDFS = Protocol.getProtocol( "hdfs" );

  public HadoopDefaultProtocolHandler()
    {
    }

  @Override
  public boolean handles( Protocol protocol )
    {
    return HDFS.equals( protocol );
    }

  @Override
  public Collection<? extends Protocol> getProtocols()
    {
    return Collections.singleton( HDFS );
    }

  @Override
  protected Tap createTapFor( Resource<Protocol, Format, SinkMode> resource, Scheme scheme )
    {
    if( resource.getProtocol().equals( HDFS ) )
      return new Hfs( scheme, resource.getIdentifier(), resource.getMode() );

    return null;
    }
  }
