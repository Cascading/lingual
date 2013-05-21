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

import cascading.bind.catalog.Stereotype;
import cascading.bind.process.FlowFactory;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.optiq.meta.Branch;
import cascading.lingual.optiq.meta.Ref;
import cascading.lingual.util.Version;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;

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

  /**
   * Instantiates a new Lingual flow factory.
   *
   * @param platformBroker the platform broker
   * @param name           the name
   * @param branch         the branch
   */
  public LingualFlowFactory( PlatformBroker platformBroker, String name, Branch branch )
    {
    super( platformBroker.getProperties(), platformBroker.getCatalog().getProtocolHandlers(), name );
    this.platformBroker = platformBroker;
    this.catalog = platformBroker.getCatalog();
    this.tail = branch.current;

    for( Ref head : branch.heads.keySet() )
      {
      Stereotype<Protocol, Format> stereotypeFor;

      if( head.identifier == null )
        stereotypeFor = catalog.getRootSchemaDef().getStereotype( head.name );
      else
        stereotypeFor = catalog.findStereotypeFor( head.identifier );

      setSourceStereotype( head.name, stereotypeFor );
      }

    setSinkStereotype( this.tail.getName(), catalog.getStereoTypeFor( Fields.UNKNOWN ) );

    AppProps.addApplicationFramework( getProperties(), Version.getName() + ":" + Version.getVersionString() );
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
