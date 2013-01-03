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

import cascading.bind.process.FlowFactory;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.optiq.meta.Branch;
import cascading.lingual.optiq.meta.Ref;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;

/**
 *
 */
public class LingualFlowFactory extends FlowFactory<Protocol, Format>
  {
  PlatformBroker platformBroker;
  private Pipe tail;
  private SchemaCatalog catalog;

  public LingualFlowFactory( PlatformBroker platformBroker, String name, Branch branch )
    {
    super( platformBroker.getProperties(), platformBroker.getCatalog().getProtocolHandlers(), name );
    this.platformBroker = platformBroker;
    this.catalog = platformBroker.getCatalog();
    this.tail = branch.current;

    for( Ref head : branch.heads.keySet() )
      setSourceStereotype( head.name, catalog.findStereotypeFor( head.identifier ) );

    setSinkStereotype( this.tail.getName(), catalog.getStereoTypeFor( Fields.UNKNOWN ) );
    }

  @Override
  protected FlowConnector getFlowConnector()
    {
    return platformBroker.getFlowConnector();
    }

  public void addSource( String sourceName, String path )
    {
    addSourceResource( sourceName, catalog.getResourceFor( path, SinkMode.KEEP ) );
    }

  public void addSink( String sinkName, String path )
    {
    addSinkResource( sinkName, catalog.getResourceFor( path, SinkMode.REPLACE ) );
    }

  @Override
  public Flow create()
    {
    FlowDef flowDef = FlowDef.flowDef()
      .setName( getName() )
      .addTails( tail )
      .setDebugLevel( platformBroker.getDebugLevel() );

    return createFlowFrom( flowDef, tail );
    }
  }
