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
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.bind.catalog.Point;
import cascading.bind.catalog.Resource;
import cascading.bind.catalog.Stereotype;
import cascading.bind.catalog.handler.SchemeHandler;
import cascading.bind.catalog.handler.SchemeHandlers;
import cascading.bind.catalog.handler.TapHandler;
import cascading.bind.catalog.handler.TapHandlers;
import cascading.lingual.jdbc.LingualConnection;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.tap.TapSchema;
import cascading.lingual.util.Util;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.hydromatic.optiq.MutableSchema;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class SchemaCatalog implements Serializable
  {
  private transient PlatformBroker platformBroker;

  @JsonProperty
  private Protocol defaultProtocol = Protocol.FILE;
  @JsonProperty
  private SchemaDef rootSchemaDef = new SchemaDef();

  private TapHandlers<Protocol, Format> tapHandlers;
  private SchemeHandlers<Protocol, Format> schemeHandlers;

  private Map<String, Fields> nameFieldsMap = new HashMap<String, Fields>();
  private Map<String, Stereotype<Protocol, Format>> idStereotypeMap = new HashMap<String, Stereotype<Protocol, Format>>();
  private Map<String, Point<Protocol, Format>> idPointMap = new HashMap<String, Point<Protocol, Format>>();

  protected SchemaCatalog()
    {
    }

  public void setPlatformBroker( PlatformBroker platformBroker )
    {
    this.platformBroker = platformBroker;
    }

  public void initialize()
    {
    if( !rootSchemaDef.hasStereotype( "dynamic" ) )
      rootSchemaDef.addStereotype( createStereotype( Protocol.FILE, "dynamic", Fields.UNKNOWN ) );
    }

  public TapHandlers<Protocol, Format> getTapHandlers()
    {
    if( tapHandlers == null )
      tapHandlers = new TapHandlers<Protocol, Format>( createTapHandlers() );

    return tapHandlers;
    }

  public SchemeHandlers<Protocol, Format> getSchemeHandlers()
    {
    if( schemeHandlers == null )
      schemeHandlers = new SchemeHandlers<Protocol, Format>( createSchemeHandlers() );

    return schemeHandlers;
    }

  public SchemaDef getRootSchemaDef()
    {
    return rootSchemaDef;
    }

  public Protocol getDefaultProtocol()
    {
    return defaultProtocol;
    }

  public void setDefaultProtocol( Protocol defaultProtocol )
    {
    this.defaultProtocol = defaultProtocol;
    }

  public Collection<String> getSchemaNames()
    {
    return rootSchemaDef.getChildSchemaNames();
    }

  public SchemaDef getSchemaDef( String name )
    {
    return rootSchemaDef.getSchema( name );
    }

  public void addSchemaDefNamed( String name )
    {
    rootSchemaDef.getOrAddSchema( name );
    }

  public boolean removeSchemaDef( String schemaName )
    {
    return rootSchemaDef.removeSchema( schemaName );
    }

  public boolean renameSchemaDef( String schemaName, String newName )
    {
    return rootSchemaDef.renameSchema( schemaName, newName );
    }

  public String createSchemaFor( String schemaIdentifier )
    {
    String schemaName = Util.createSchemaNameFrom( schemaIdentifier );
    String[] childIdentifiers = getChildIdentifiers( schemaIdentifier );

    for( String identifier : childIdentifiers )
      createTableFor( schemaName, identifier );

    return schemaName;
    }

  private String[] getChildIdentifiers( String schemaIdentifier )
    {
    try
      {
      return platformBroker.getChildIdentifiers( schemaIdentifier );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to find children for: " + schemaIdentifier, exception );
      }
    }

  public Collection<String> getTableNames( String schemaName )
    {
    return rootSchemaDef.getSchema( schemaName ).getChildTableNames();
    }

  public String createTableFor( String schemaName, String identifier )
    {
    SchemaDef schemaDef = rootSchemaDef.getOrAddSchema( schemaName );

    return createTableFor( schemaDef, identifier );
    }

  public void createTableFor( String identifier )
    {
    createTableFor( rootSchemaDef, identifier );
    }

  public boolean removeTableDef( String schemaName, String tableName )
    {
    return rootSchemaDef.removeTable( schemaName, tableName );
    }

  public boolean renameTableDef( String schemaName, String tableName, String renameName )
    {
    return rootSchemaDef.renameTable( schemaName, tableName, renameName );
    }

  protected String createTableFor( SchemaDef schema, String identifier )
    {
    String tableName = Util.createTableNameFrom( identifier );
    Stereotype<Protocol, Format> stereotype = getStereotypeFor( identifier );

    schema.addTable( tableName, identifier, stereotype );

    return tableName;
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

  public Collection<String> getFormatNames( String schemaName )
    {
    return rootSchemaDef.getSchema( schemaName ).getFormatNames();
    }

  public Collection<String> getProtocolNames( String schemaName )
    {
    return rootSchemaDef.getSchema( schemaName ).getProtocolNames();
    }

  private Stereotype<Protocol, Format> createStereotype( Protocol protocol, String name, Fields fields )
    {
    return new Stereotype<Protocol, Format>( getSchemeHandlers(), protocol, name, fields );
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
      stereotype = createStereotype( getProtocolFor( identifier ), name, fields );

      rootSchemaDef.addStereotype( stereotype );
      }

    idStereotypeMap.put( identifier, stereotype );

    return stereotype;
    }

  public Stereotype getStereoTypeFor( Fields fields )
    {
    return rootSchemaDef.getStereotypeFor( fields );
    }

  public Fields getFieldsFor( String identifier )
    {
    String name = Util.createTableNameFrom( identifier );

    if( nameFieldsMap.containsKey( name ) )
      return nameFieldsMap.get( name );

    Protocol protocol = getDefaultProtocolFor( identifier );
    Format format = getDefaultFormatFor( identifier );

    Resource<Protocol, Format, SinkMode> resource = getResourceFor( identifier, protocol, format, SinkMode.KEEP );

    Tap tap = createTapFor( rootSchemaDef.getStereotypeFor( Fields.UNKNOWN ), resource );
    Fields fields = tap.retrieveSourceFields( platformBroker.getFlowProcess() );

    nameFieldsMap.put( name, fields );

    return fields;
    }

  private Tap createTapFor( Stereotype<Protocol, Format> stereotype, Resource<Protocol, Format, SinkMode> resource )
    {
    TapHandler<Protocol, Format> tapHandler = getTapHandlers().findHandlerFor( resource.getProtocol() );

    if( tapHandler != null )
      return tapHandler.createTap( stereotype, resource );

    return null;
    }

  public Resource<Protocol, Format, SinkMode> getResourceFor( String identifier, SinkMode mode )
    {
    Point<Protocol, Format> point = getPointFor( identifier, null, null );

    return getResourceFor( identifier, point.protocol, point.format, mode );
    }

  private Resource<Protocol, Format, SinkMode> getResourceFor( String identifier, Protocol protocol, Format format, SinkMode mode )
    {
    return new Resource<Protocol, Format, SinkMode>( identifier, protocol, format, mode );
    }

  protected abstract List<TapHandler<Protocol, Format>> createTapHandlers();

  protected abstract List<SchemeHandler<Protocol, Format>> createSchemeHandlers();
  }
