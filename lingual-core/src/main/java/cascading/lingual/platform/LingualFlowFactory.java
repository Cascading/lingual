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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import cascading.bind.catalog.Resource;
import cascading.bind.process.FlowFactory;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.jdbc.LingualConnection;
import cascading.lingual.util.Version;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.tap.SinkMode;

/**
 * Class LingualFlowFactory is an implementation of the {@link FlowFactory} base class.
 * <p/>
 * This FlowFactory can dynamically resolve source and sink taps across any platform and return a {@link Flow} for
 * that platform without any coupling to a given platform implementation.
 */
public class LingualFlowFactory extends FlowFactory<Protocol, Format>
  {
  PlatformBroker platformBroker;
  private Pipe tail;
  private SchemaCatalog catalog;
  private LingualConnection lingualConnection;
  private Set<String> jars = new HashSet<String>();

  /**
   * Instantiates a new Lingual flow factory.
   *
   * @param platformBroker the platform broker
   * @param name           the name
   * @param tail
   */
  public LingualFlowFactory( PlatformBroker platformBroker, LingualConnection lingualConnection, String name, Pipe tail )
    {
    super( platformBroker.getProperties(), name );
    this.platformBroker = platformBroker;
    this.catalog = platformBroker.getCatalog();
    this.lingualConnection = lingualConnection;
    this.tail = tail;

    AppProps.addApplicationFramework( getProperties(), Version.getName() + ":" + Version.getVersionString() );
    }

  @Override
  protected FlowConnector getFlowConnector()
    {
    return platformBroker.getFlowConnector();
    }

  public void addSource( String sourceName, String path, String... jarPaths )
    {
    addJars( jarPaths );

    Resource<Protocol, Format, SinkMode> resource = catalog.getResourceFor( path, SinkMode.KEEP );

    addSourceResource( sourceName, resource );
    }

  public void addSink( String sinkName, String path, String... jarPaths )
    {
    addJars( jarPaths );

    Resource<Protocol, Format, SinkMode> resourceFor = catalog.getResourceFor( path, SinkMode.REPLACE );

    addSinkResource( sinkName, resourceFor );
    }

  private void addJars( String... jarPaths )
    {
    Collections.addAll( jars, jarPaths );
    }

  @Override
  public Flow create()
    {
    FlowDef flowDef = FlowDef.flowDef()
      .setName( getName() )
      .addTails( tail )
      .setDebugLevel( platformBroker.getDebugLevel() );

    Flow flow = createFlowFrom( flowDef, tail );

    flow.addListener( new LingualConnectionFlowListener( lingualConnection ) );

    return flow;
    }
  }
