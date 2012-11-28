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
import cascading.bind.catalog.handler.FormatHandler;
import cascading.bind.catalog.handler.FormatHandlers;
import cascading.bind.catalog.handler.ProtocolHandler;
import cascading.bind.catalog.handler.ProtocolHandlers;
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
  private Protocol defaultProtocol;
  @JsonProperty
  private Format defaultFormat;
  @JsonProperty
  private SchemaDef rootSchemaDef = new SchemaDef();

  private ProtocolHandlers<Protocol, Format> protocolHandlers;
  private FormatHandlers<Protocol, Format> formatHandlers;

  private Map<String, Fields> nameFieldsMap = new HashMap<String, Fields>();
  private Map<String, Point<Protocol, Format>> idPointMap = new HashMap<String, Point<Protocol, Format>>();

  protected SchemaCatalog( Protocol defaultProtocol, Format defaultFormat )
    {
    this.defaultProtocol = defaultProtocol;
    this.defaultFormat = defaultFormat;
    }

  public void setPlatformBroker( PlatformBroker platformBroker )
    {
    this.platformBroker = platformBroker;
    }

  public void initializeNew()
    {
    if( !rootSchemaDef.hasStereotype( "UNKNOWN" ) )
      createStereotype( rootSchemaDef, defaultProtocol, "UNKNOWN", Fields.UNKNOWN );

    ProtocolHandlers<Protocol, Format> protocolHandlers = getProtocolHandlers();

    for( Protocol protocol : protocolHandlers.getProtocols() )
      rootSchemaDef.addProtocolProperties( protocol, protocolHandlers.getProtocolProperties( protocol ) );

    FormatHandlers<Protocol, Format> formatHandlers = getFormatHandlers();

    for( Format format : formatHandlers.getFormats() )
      rootSchemaDef.addFormatProperties( format, formatHandlers.getFormatProperties( format ) );
    }

  public ProtocolHandlers<Protocol, Format> getProtocolHandlers()
    {
    if( protocolHandlers == null )
      protocolHandlers = new ProtocolHandlers<Protocol, Format>( createProtocolHandlers() );

    return protocolHandlers;
    }

  public FormatHandlers<Protocol, Format> getFormatHandlers()
    {
    if( formatHandlers == null )
      formatHandlers = new FormatHandlers<Protocol, Format>( createFormatHandlers() );

    return formatHandlers;
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

  public Format getDefaultFormat()
    {
    return defaultFormat;
    }

  public void setDefaultFormat( Format defaultFormat )
    {
    this.defaultFormat = defaultFormat;
    }

  public Collection<String> getSchemaNames()
    {
    return rootSchemaDef.getChildSchemaNames();
    }

  public SchemaDef getSchemaDef( String name )
    {
    if( name == null )
      return rootSchemaDef;

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

  public String createSchemaDefAndTableDefsFor( String schemaIdentifier )
    {
    String schemaName = Util.createSchemaNameFrom( schemaIdentifier );
    SchemaDef schemaDef = rootSchemaDef.getOrAddSchema( schemaName );

    String[] childIdentifiers = getChildIdentifiers( schemaIdentifier );

    for( String identifier : childIdentifiers )
      createTableDefFor( schemaDef, null, identifier, null, null );

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
    return getSchemaDef( schemaName ).getChildTableNames();
    }

  public void createTableDefFor( String identifier )
    {
    createTableDefFor( rootSchemaDef, null, identifier, null, null );
    }

  public boolean removeTableDef( String schemaName, String tableName )
    {
    return rootSchemaDef.removeTable( schemaName, tableName );
    }

  public boolean renameTableDef( String schemaName, String tableName, String renameName )
    {
    return rootSchemaDef.renameTable( schemaName, tableName, renameName );
    }

  protected String createTableDefFor( SchemaDef schemaDef, String tableName, String identifier, Protocol protocol, Format format )
    {
    if( tableName == null )
      tableName = Util.createTableNameFrom( identifier );

    Stereotype<Protocol, Format> stereotype = getOrCreateStereotype( schemaDef, identifier );

    schemaDef.addTable( tableName, identifier, stereotype, protocol, format );

    return tableName;
    }

  private Stereotype<Protocol, Format> getOrCreateStereotype( SchemaDef schema, String identifier )
    {
    Stereotype<Protocol, Format> stereotype = findStereotypeFor( identifier );

    if( stereotype != null )
      return stereotype;

    Fields fields = getFieldsFor( identifier );

    stereotype = schema.getStereotypeFor( fields );

    if( stereotype == null )
      {
      String name = Util.createTableNameFrom( identifier );
      Point<Protocol, Format> point = getPointFor( identifier, schema.getName(), null, null );

      return createStereotype( schema, point.protocol, name, fields );
      }

    return stereotype;
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

  protected Point<Protocol, Format> getPointFor( String identifier, String schemaName, Protocol protocol, Format format )
    {
    if( idPointMap.containsKey( identifier ) )
      return idPointMap.get( identifier );

    Point<Protocol, Format> point = createPointFor( identifier, protocol, format, schemaName );

    idPointMap.put( identifier, point );

    return point;
    }

  private Point<Protocol, Format> createPointFor( String identifier, Protocol protocol, Format format, String schemaName )
    {
    if( protocol == null )
      protocol = getDefaultProtocolFor( identifier );

    if( format == null )
      format = getDefaultFormatFor( identifier, schemaName );

    return new Point<Protocol, Format>( protocol, format );
    }

  public Protocol getDefaultProtocolFor( String identifier )
    {
    TableDef tableDef = rootSchemaDef.findTableFor( identifier );

    if( tableDef != null && tableDef.getProtocol() != null )
      return tableDef.getProtocol();

    return defaultProtocol;
    }

  public Format getDefaultFormatFor( String identifier, String schemaName )
    {
    TableDef tableDef = rootSchemaDef.findTableFor( identifier );

    if( tableDef != null && tableDef.getFormat() != null )
      return tableDef.getFormat();

    Format format = FormatProperties.findFormatFor( getSchemaDef( schemaName ), identifier );

    if( format == null )
      format = defaultFormat;

    return format;
    }

  public Collection<String> getStereotypeNames()
    {
    return rootSchemaDef.getStereotypeNames();
    }

  public boolean removeStereotype( String schemaName, String stereotypeName )
    {
    SchemaDef schema = rootSchemaDef;

    if( schemaName != null )
      schema = rootSchemaDef.getSchema( schemaName );

    return schema.removeStereotype( stereotypeName );
    }

  public boolean renameStereotype( String schemaName, String name, String newName )
    {
    SchemaDef schema = rootSchemaDef;

    if( schemaName != null )
      schema = rootSchemaDef.getSchema( schemaName );

    return schema.renameStereotype( name, newName );
    }

  public Stereotype<Protocol, Format> findStereotypeFor( String identifier )
    {
    TableDef tableDef = rootSchemaDef.findTableFor( identifier );

    if( tableDef == null )
      return null;

    return tableDef.getStereotype();
    }

  public boolean createStereotype( String schemaName, String name, Fields fields )
    {
    SchemaDef schemaDef = getSchemaDef( schemaName );

    return createStereotype( schemaDef, defaultProtocol, name, fields ) != null;
    }

  private Stereotype<Protocol, Format> createStereotype( SchemaDef schemaDef, Protocol defaultProtocol, String name, Fields fields )
    {
    Stereotype<Protocol, Format> stereotype = new Stereotype<Protocol, Format>( getFormatHandlers(), defaultProtocol, name, fields );

    schemaDef.addStereotype( stereotype );

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

    Point<Protocol, Format> point = getPointFor( identifier, null, null, null );

    Resource<Protocol, Format, SinkMode> resource = new Resource<Protocol, Format, SinkMode>( identifier, point.protocol, point.format, SinkMode.KEEP );

    Tap tap = createTapFor( rootSchemaDef.getStereotypeFor( Fields.UNKNOWN ), resource );
    Fields fields = tap.retrieveSourceFields( platformBroker.getFlowProcess() );

    nameFieldsMap.put( name, fields );

    return fields;
    }

  private Tap createTapFor( Stereotype<Protocol, Format> stereotype, Resource<Protocol, Format, SinkMode> resource )
    {
    ProtocolHandler<Protocol, Format> protocolHandler = getProtocolHandlers().findHandlerFor( resource.getProtocol() );

    if( protocolHandler != null )
      return protocolHandler.createTap( stereotype, resource );

    return null;
    }

  public Resource<Protocol, Format, SinkMode> getResourceFor( String identifier, SinkMode mode )
    {
    Point<Protocol, Format> point = getPointFor( identifier, null, null, null );
    Protocol protocol = point.protocol;
    Format format = point.format;

    return new Resource<Protocol, Format, SinkMode>( identifier, protocol, format, mode );
    }

  protected abstract List<ProtocolHandler<Protocol, Format>> createProtocolHandlers();

  protected abstract List<FormatHandler<Protocol, Format>> createFormatHandlers();

  public void addFormat( String schemaName, Format format, List<String> extensions )
    {
    SchemaDef schemaDef = getSchemaDef( schemaName );

    schemaDef.addFormatProperty( format, FormatProperties.EXTENSIONS, extensions );
    }

  public void addProtocol( String schemaName, Protocol format, List<String> uris )
    {
    SchemaDef schemaDef = getSchemaDef( schemaName );

    schemaDef.addProtocolProperty( format, FormatProperties.EXTENSIONS, uris );
    }
  }
