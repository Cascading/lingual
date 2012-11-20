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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import cascading.bind.catalog.DynamicStereotype;
import cascading.bind.catalog.Point;
import cascading.bind.catalog.Stereotype;
import cascading.bind.tap.TapResource;
import cascading.lingual.jdbc.LingualConnection;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.tap.TapSchema;
import cascading.lingual.util.Util;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import net.hydromatic.optiq.MutableSchema;

/**
 *
 */
public abstract class SchemaCatalog
  {
  private final SchemaDef rootSchemaDef = new SchemaDef();

  private final PlatformBroker platformBroker;

  private Protocol defaultProtocol = Protocol.FILE;

  private Map<String, Fields> nameFieldsMap = new HashMap<String, Fields>();
  private Map<String, Stereotype<Protocol, Format>> idStereotypeMap = new HashMap<String, Stereotype<Protocol, Format>>();
  private Map<String, Point<Protocol, Format>> idPointMap = new HashMap<String, Point<Protocol, Format>>();

  protected SchemaCatalog( PlatformBroker platformBroker )
    {
    this.platformBroker = platformBroker;

    initialize();
    }

  protected void initialize()
    {
    rootSchemaDef.addStereotype( createDynamicSchemaFor( Protocol.FILE, "dynamic", Fields.UNKNOWN ) );
    }

  public void addSchemaFor( String schemaIdentifier ) throws IOException
    {
    String schemaName = Util.createSchemaNameFrom( schemaIdentifier );
    String[] childIdentifiers = platformBroker.getChildIdentifiers( schemaIdentifier );

    for( String identifier : childIdentifiers )
      addTableFor( schemaName, identifier );
    }

  protected void addTableFor( String schemaName, String identifier )
    {
    SchemaDef schemaDef = rootSchemaDef.getOrAddSchema( schemaName );

    addTableFor( schemaDef, identifier );
    }

  public void addTableFor( String identifier )
    {
    addTableFor( rootSchemaDef, identifier );
    }

  protected void addTableFor( SchemaDef schema, String identifier )
    {
    String tableName = Util.createTableNameFrom( identifier );
    Stereotype<Protocol, Format> stereotype = getStereotypeFor( identifier );

    schema.addTable( tableName, identifier, stereotype );
    }

  public void addSchemasTo( LingualConnection connection ) throws SQLException
    {
    MutableSchema rootSchema = connection.getRootSchema();
    SchemaDef currentSchemaDef = rootSchemaDef;

    addSchemas( connection, rootSchema, currentSchemaDef );
    }

  private void addSchemas( LingualConnection connection, MutableSchema currentSchema, SchemaDef currentSchemaDef )
    {
    Collection<SchemaDef> schemaDefs = currentSchemaDef.getChildSchemaDefs();

    for( SchemaDef childSchemaDef : schemaDefs )
      {
      TapSchema childSchema = new TapSchema( connection, childSchemaDef );
      addSchemas( connection, childSchema, childSchemaDef );

      currentSchema.addSchema( childSchemaDef.getName(), childSchema );
      }
    }

  private DynamicStereotype<Protocol, Format> createDynamicSchemaFor( Protocol protocol, String name, Fields fields )
    {
    DynamicStereotype<Protocol, Format> stereotype = new DynamicStereotype<Protocol, Format>( protocol, name, fields );

    stereotype.addSchemeFactory( getSchemeFactory(), Protocol.FILE, Format.CSV );
    stereotype.addSchemeFactory( getSchemeFactory(), Protocol.FILE, Format.TSV );
    stereotype.addSchemeFactory( getSchemeFactory(), Protocol.FILE, Format.TCSV );
    stereotype.addSchemeFactory( getSchemeFactory(), Protocol.FILE, Format.TTSV );

    return stereotype;
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

  public Stereotype<Protocol, Format> getStereotypeFor( String identifier )
    {
    if( idStereotypeMap.containsKey( identifier ) )
      return idStereotypeMap.get( identifier );

    Fields fields = getFieldsFor( identifier );
    Stereotype<Protocol, Format> stereotype = rootSchemaDef.getStereotypeFor( fields );

    if( stereotype == null )
      {
      addIdentifier( identifier, null, null );
      String name = Util.createTableNameFrom( identifier );
      stereotype = createDynamicSchemaFor( getProtocolFor( identifier ), name, fields );

      rootSchemaDef.addStereotype( stereotype );
      }

    idStereotypeMap.put( identifier, stereotype );

    return stereotype;
    }

  public Stereotype getStereoTypeFor( Fields fields )
    {
    return rootSchemaDef.getStereotypeFor( fields );
    }

  protected abstract DynamicStereotype.SchemeFactory getSchemeFactory();

  public Fields getFieldsFor( String identifier )
    {
    String name = Util.createTableNameFrom( identifier );

    if( nameFieldsMap.containsKey( name ) )
      return nameFieldsMap.get( name );

    Protocol protocol = getDefaultProtocolFor( identifier );
    Format format = getDefaultFormatFor( identifier );

    Tap tap = getResourceFor( identifier, protocol, format, SinkMode.KEEP ).createTapFor( rootSchemaDef.getStereotypeFor( Fields.UNKNOWN ) );
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
