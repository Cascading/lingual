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

package cascading.lingual.catalog;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import cascading.bind.catalog.Catalog;
import cascading.bind.catalog.DynamicSchema;
import cascading.bind.catalog.Point;
import cascading.bind.catalog.Schema;
import cascading.bind.tap.TapResource;
import cascading.lingual.jdbc.LingualConnection;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.tap.TapSchema;
import cascading.lingual.util.Util;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 *
 */
public abstract class LingualCatalog extends Catalog<Protocol, Format>
  {
  private final PlatformBroker platformBroker;

  private Protocol defaultProtocol = Protocol.FILE;

  private Map<String, Fields> nameFieldsMap = new HashMap<String, Fields>();
  private Map<String, Schema<Protocol, Format>> idSchemaMap = new HashMap<String, Schema<Protocol, Format>>();
  private Map<String, Point<Protocol, Format>> idPointMap = new HashMap<String, Point<Protocol, Format>>();

  protected LingualCatalog( PlatformBroker platformBroker )
    {
    this.platformBroker = platformBroker;

    initialize();
    }

  protected void initialize()
    {
    addSchema( createDynamicSchemaFor( Protocol.FILE, "dynamic", Fields.UNKNOWN ) );
    }

  public void addSchemaFor( LingualConnection connection, String schemaIdentifier ) throws SQLException
    {
    try
      {
      TapSchema.create( connection, schemaIdentifier );
      }
    catch( IOException exception )
      {
      throw new SQLException( exception );
      }
    }

  private DynamicSchema<Protocol, Format> createDynamicSchemaFor( Protocol protocol, String name, Fields fields )
    {
    DynamicSchema<Protocol, Format> schema = new DynamicSchema<Protocol, Format>( protocol, name, fields );

    schema.addSchemeFactory( getSchemeFactory(), Protocol.FILE, Format.CSV );
    schema.addSchemeFactory( getSchemeFactory(), Protocol.FILE, Format.TSV );
    schema.addSchemeFactory( getSchemeFactory(), Protocol.FILE, Format.TCSV );
    schema.addSchemeFactory( getSchemeFactory(), Protocol.FILE, Format.TTSV );

    return schema;
    }

  public void addIdentifier( String identifier, Protocol protocol, Format format )
    {
    idPointMap.put( identifier, createPointFor( identifier, protocol, format ) );
    }

  protected Point<Protocol, Format> getPointFor( String identifier, Protocol protocol, Format format )
    {
    if( idPointMap.containsKey( identifier ) )
      return idPointMap.get( identifier );

    Point<Protocol, Format> point = createPointFor( identifier, protocol, format );
    idPointMap.put( identifier, point );

    return point;
    }

  public Protocol getProtocolFor( String identifier )
    {
    if( idPointMap.containsKey( identifier ) )
      return idPointMap.get( identifier ).protocol;

    return getDefaultProtocolFor( identifier );
    }

  public Format getFormatFor( String identifier )
    {
    if( idPointMap.containsKey( identifier ) )
      return idPointMap.get( identifier ).format;

    return getDefaultFormatFor( identifier );
    }

  private Point<Protocol, Format> createPointFor( String identifier, Protocol protocol, Format format )
    {
    if( protocol == null )
      protocol = getDefaultProtocolFor( identifier );

    if( format == null )
      format = getDefaultFormatFor( identifier );

    return new Point<Protocol, Format>( protocol, format );
    }

  public Protocol getDefaultProtocolFor( String identifier )
    {
    return defaultProtocol;
    }

  public Format getDefaultFormatFor( String identifier )
    {
    Format format = Format.findFormatFor( identifier );

    if( format == null )
      format = Format.TCSV;

    return format;
    }

  public Schema<Protocol, Format> getSchemaFor( String identifier )
    {
    if( idSchemaMap.containsKey( identifier ) )
      return idSchemaMap.get( identifier );

    Fields fields = getFieldsFor( identifier );
    Schema<Protocol, Format> schema = getSchemaFor( fields );

    if( schema == null )
      {
      addIdentifier( identifier, null, null );
      String name = Util.createTableNameFrom( identifier );
      schema = createDynamicSchemaFor( getProtocolFor( identifier ), name, fields );

      addSchema( schema );
      }

    idSchemaMap.put( identifier, schema );

    return schema;
    }

  protected abstract DynamicSchema.SchemeFactory getSchemeFactory();

  public Fields getFieldsFor( String identifier )
    {
    String name = Util.createTableNameFrom( identifier );

    if( nameFieldsMap.containsKey( name ) )
      return nameFieldsMap.get( name );

    Protocol protocol = getDefaultProtocolFor( identifier );
    Format format = getDefaultFormatFor( identifier );

    Tap tap = getResourceFor( identifier, protocol, format, SinkMode.KEEP ).createTapFor( getSchemaFor( Fields.UNKNOWN ) );
    Fields fields = tap.retrieveSourceFields( platformBroker.getFlowProcess() );

    nameFieldsMap.put( name, fields );

    return fields;
    }

  public TapResource getResourceFor( String identifier, SinkMode mode )
    {
    Point<Protocol, Format> point = getPointFor( identifier, null, null );

    return getResourceFor( identifier, point.protocol, point.format, mode );
    }

  public abstract TapResource getResourceFor( String identifier, Protocol protocol, Format format, SinkMode mode );
  }
