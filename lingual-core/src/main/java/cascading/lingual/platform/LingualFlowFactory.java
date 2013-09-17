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
import java.util.Properties;
import java.util.Set;

import cascading.bind.catalog.Resource;
import cascading.bind.process.FlowFactory;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.TableDef;
import cascading.lingual.jdbc.Driver;
import cascading.lingual.jdbc.LingualConnection;
import cascading.lingual.util.Version;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.tap.SinkMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class LingualFlowFactory is an implementation of the {@link FlowFactory} base class.
 * <p/>
 * This FlowFactory can dynamically resolve source and sink taps across any platform and return a {@link Flow} for
 * that platform without any coupling to a given platform implementation.
 */
public class LingualFlowFactory extends FlowFactory<Protocol, Format>
  {
  private static final Logger LOG = LoggerFactory.getLogger( LingualFlowFactory.class );

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
    super( new Properties( platformBroker.getProperties() ), name );
    this.platformBroker = platformBroker;
    this.catalog = platformBroker.getCatalog();
    this.lingualConnection = lingualConnection;
    this.tail = tail;

    AppProps.addApplicationFramework( getProperties(), Version.getName() + ":" + Version.getVersionString() );
    AppProps.addApplicationTag( getProperties(), getProperties().getProperty( Driver.TAGS_PROP ) );
    }

  @Override
  protected FlowConnector getFlowConnector()
    {
    return platformBroker.getFlowConnector();
    }

  public void addSource( String sourceName, TableDef tableDef, String... jarPaths )
    {
    addJars( jarPaths );

    addSourceResource( sourceName, catalog.getResourceFor( tableDef, SinkMode.KEEP ) );
    }

  public void addSink( String sinkName, Resource resource )
    {
    addSinkResource( sinkName, resource );
    }

  public void addSink( String sinkName, String identifier )
    {
    addSinkResource( sinkName, catalog.getResourceFor( identifier, SinkMode.REPLACE ) );
    }

  public void addSink( String sinkName, TableDef tableDef, String... jarPaths )
    {
    addJars( jarPaths );

    addSinkResource( sinkName, catalog.getResourceFor( tableDef, SinkMode.REPLACE ) );
    }

  private void addJars( String... jarPaths )
    {
    Collections.addAll( jars, jarPaths );
    }

  public Set<String> getJars()
    {
    return jars;
    }

  public String[] getJarsArray()
    {
    return jars.toArray( new String[ jars.size() ] );
    }

  public LingualConnection getLingualConnection()
    {
    return lingualConnection;
    }

  @Override
  public Flow create()
    {
    FlowDef flowDef = FlowDef.flowDef()
      .setName( getName() )
      .addTails( tail )
      .setDebugLevel( platformBroker.getDebugLevel() );

    LOG.debug( "using log level: {}", platformBroker.getDebugLevel() );

    for( String jar : jars )
      {
      LOG.debug( "adding jar to classpath: {}", jar );
      flowDef.addToClassPath( jar );
      }

    Flow flow = createFlowFrom( flowDef, tail );

    flow.addListener( new LingualConnectionFlowListener( lingualConnection ) );

    return flow;
    }
  }
