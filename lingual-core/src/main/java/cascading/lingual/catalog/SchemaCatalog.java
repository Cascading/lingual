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

package cascading.lingual.catalog;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Collection;
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
import cascading.lingual.util.InsensitiveMap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.hydromatic.optiq.impl.java.MapSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@JsonAutoDetect(
  fieldVisibility = JsonAutoDetect.Visibility.ANY,
  getterVisibility = JsonAutoDetect.Visibility.NONE,
  setterVisibility = JsonAutoDetect.Visibility.NONE,
  isGetterVisibility = JsonAutoDetect.Visibility.NONE
)
public abstract class SchemaCatalog implements Serializable
  {
  private static final Logger LOG = LoggerFactory.getLogger( SchemaCatalog.class );

  private transient PlatformBroker platformBroker;

  @JsonProperty
  private SchemaDef rootSchemaDef;

  @JsonProperty
  private ProtocolHandlers<Protocol, Format> protocolHandlers;
  @JsonProperty
  private FormatHandlers<Protocol, Format> formatHandlers;

  @JsonProperty
  private Map<String, Fields> nameFieldsMap = new InsensitiveMap<Fields>();

  @JsonProperty
  private Map<String, Point<Protocol, Format>> idPointMap = new InsensitiveMap<Point<Protocol, Format>>();

  protected SchemaCatalog()
    {
    }

  protected SchemaCatalog( Protocol defaultProtocol, Format defaultFormat )
    {
    this.rootSchemaDef = new SchemaDef( defaultProtocol, defaultFormat );
    }

  public void setPlatformBroker( PlatformBroker platformBroker )
    {
    this.platformBroker = platformBroker;
    }

  public void initializeNew()
    {
    if( !rootSchemaDef.hasStereotype( "UNKNOWN" ) )
      createStereotype( rootSchemaDef, "UNKNOWN", Fields.UNKNOWN, null );

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

  public TableDef resolveTableDef( String[] names )
    {
    if( names.length == 0 )
      return null;

    SchemaDef current = getRootSchemaDef();

    for( int i = 0; i < names.length - 1; i++ )
      current = current.getSchema( names[ i ] );

    return current.getTable( names[ names.length - 1 ] );
    }

  public Collection<String> getSchemaNames()
    {
    return getRootSchemaDef().getChildSchemaNames();
    }

  public SchemaDef getSchemaDef( String name )
    {
    if( name == null )
      return getRootSchemaDef();

    return getRootSchemaDef().getSchema( name );
    }

  public boolean addSchemaDef( String name, String protocolName, String formatName )
    {
    Protocol protocol = Protocol.getProtocol( protocolName );
    Format format = Format.getFormat( formatName );

    return getRootSchemaDef().addSchema( name, protocol, format );
    }

  public SchemaDef createSchemaDef( String name, String protocolName, String formatName, String identifier )
    {
    Protocol protocol = Protocol.getProtocol( protocolName );
    Format format = Format.getFormat( formatName );

    if( identifier == null )
      identifier = name;

    getRootSchemaDef().addSchema( name, protocol, format, identifier );

    return getRootSchemaDef().getSchema( name );
    }

  public boolean removeSchemaDef( String schemaName )
    {
    return getRootSchemaDef().removeSchema( schemaName );
    }

  public boolean renameSchemaDef( String schemaName, String newName )
    {
    return getRootSchemaDef().renameSchema( schemaName, newName );
    }

  public String createSchemaDefAndTableDefsFor( String schemaIdentifier )
    {
    return createSchemaDefAndTableDefsFor( null, null, null, schemaIdentifier );
    }

  public String createSchemaDefAndTableDefsFor( String schemaName, String protocolName, String formatName, String schemaIdentifier )
    {
    schemaIdentifier = getFullPath( schemaIdentifier );

    LOG.debug( "using schema full path: {}", schemaIdentifier );

    if( schemaName == null )
      schemaName = platformBroker.createSchemaNameFrom( schemaIdentifier );

    SchemaDef schemaDef = getSchemaDef( schemaName );

    if( schemaDef == null )
      schemaDef = createSchemaDef( schemaName, protocolName, formatName, schemaIdentifier );
    else if( !schemaIdentifier.equalsIgnoreCase( schemaDef.getIdentifier() ) )
      throw new IllegalArgumentException( "schema exists: " + schemaName + ", with differing identifier: " + schemaIdentifier );

    if( !platformBroker.pathExists( schemaIdentifier ) )
      return schemaName;

    String[] childIdentifiers = getChildIdentifiers( schemaIdentifier );

    LOG.debug( "schema {} has {} children", schemaName, childIdentifiers.length );

    for( String identifier : childIdentifiers )
      createTableDefFor( schemaDef, null, identifier, null, null, null, null );

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
    SchemaDef schema = getSchemaDef( schemaName );

    if( schema == null )
      throw new IllegalArgumentException( "schema does not exist: " + schemaName );

    return schema.getChildTableNames();
    }

  public void createTableDefFor( String identifier )
    {
    createTableDefFor( getRootSchemaDef(), null, identifier, null, null, null, null );
    }

  public void createTableDefFor( String schemaName, String tableName, String tableIdentifier, Fields fields, String protocolName, String formatName )
    {
    tableIdentifier = getFullPath( tableIdentifier );

    Point<Protocol, Format> point = getPointFor( tableIdentifier, schemaName, Protocol.getProtocol( protocolName ), Format.getFormat( formatName ) );

    SchemaDef schemaDef = rootSchemaDef.getSchema( schemaName );

    if( schemaDef == null )
      throw new IllegalStateException( "no schema for: " + schemaName );

    createTableDefFor( schemaDef, tableName, tableIdentifier, null, fields, point.protocol, point.format );
    }

  public boolean removeTableDef( String schemaName, String tableName )
    {
    return getRootSchemaDef().removeTable( schemaName, tableName );
    }

  public boolean renameTableDef( String schemaName, String tableName, String renameName )
    {
    return getRootSchemaDef().renameTable( schemaName, tableName, renameName );
    }

  public String createTableDefFor( String schemaName, String tableName, String identifier, String stereotypeName, Protocol protocol, Format format )
    {
    SchemaDef schemaDef;

    if( schemaName == null )
      schemaDef = getRootSchemaDef();
    else
      schemaDef = getRootSchemaDef().getSchema( schemaName );

    if( schemaDef == null )
      throw new IllegalStateException( "schema does not exist: " + schemaName );

    return createTableDefFor( schemaDef, tableName, identifier, stereotypeName, null, protocol, format );
    }

  protected String createTableDefFor( SchemaDef schemaDef, String tableName, String tableIdentifier, String stereotypeName, Fields fields, Protocol protocol, Format format )
    {
    tableIdentifier = getFullPath( tableIdentifier );

    LOG.debug( "using table full path: {}", tableIdentifier );

    if( tableName == null )
      tableName = platformBroker.createTableNameFrom( tableIdentifier );

    Stereotype<Protocol, Format> stereotype = null;

    if( stereotypeName != null )
      stereotype = findStereotype( schemaDef, stereotypeName );

    if( stereotype == null )
      stereotype = findOrCreateStereotype( schemaDef, fields, tableIdentifier );

    if( stereotype == null )
      throw new IllegalArgumentException( "stereotype does not exist: " + stereotypeName );

    schemaDef.addTable( tableName, tableIdentifier, stereotype, protocol, format );

    return tableName;
    }

  private String getFullPath( String identifier )
    {
    if( platformBroker == null )
      return identifier;

    return platformBroker.getFullPath( identifier );
    }

  private Stereotype<Protocol, Format> findOrCreateStereotype( SchemaDef schema, Fields fields, String identifier )
    {
    Stereotype<Protocol, Format> stereotype = findStereotypeFor( identifier );

    if( stereotype != null )
      return stereotype;

    if( fields == null )
      fields = getFieldsFor( identifier );

    if( fields == null )
      return schema.findStereotypeFor( Fields.UNKNOWN );

    String schemaName = schema.getName();
    String stereotypeName = platformBroker.createTableNameFrom( identifier );

    stereotype = schema.findStereotypeFor( fields );

    if( stereotype == null )
      stereotype = createStereotype( schema, stereotypeName, fields, getPointFor( identifier, schemaName ).protocol );

    return stereotype;
    }

  public void addSchemasTo( LingualConnection connection ) throws SQLException
    {
    MapSchema rootSchema = (MapSchema) connection.getRootSchema();

    addSchemas( connection, rootSchema, rootSchemaDef );
    }

  private void addSchemas( LingualConnection connection, MapSchema currentSchema, SchemaDef currentSchemaDef )
    {
    Collection<SchemaDef> schemaDefs = currentSchemaDef.getChildSchemas();

    for( SchemaDef childSchemaDef : schemaDefs )
      {
      TapSchema childSchema = (TapSchema) currentSchema.getSubSchema( childSchemaDef.getName() );

      if( childSchema == null )
        {
        childSchema = new TapSchema( currentSchema, connection, childSchemaDef );
        currentSchema.addSchema( childSchemaDef.getName(), childSchema );

        String childSchemaDescription = String.format( "%s ( %s )", childSchemaDef.getName(), currentSchemaDef.getIdentifier() );

        LOG.info( "added schema {} to {}", childSchemaDescription, currentSchemaDef.getName() );
        }

      childSchema.addTapTablesFor( childSchemaDef );

      addSchemas( connection, childSchema, childSchemaDef );
      }
    }

  public Collection<String> getFormatNames()
    {
    return rootSchemaDef.getFormatNames();
    }

  public Collection<String> getFormatNames( String schemaName )
    {
    return rootSchemaDef.getSchema( schemaName ).getFormatNames();
    }

  public Collection<String> getProtocolNames()
    {
    return rootSchemaDef.getProtocolNames();
    }

  public Collection<String> getProtocolNames( String schemaName )
    {
    return rootSchemaDef.getSchema( schemaName ).getProtocolNames();
    }

  protected Point<Protocol, Format> getPointFor( String identifier, String schemaName )
    {
    return getPointFor( identifier, schemaName, null, null );
    }

  protected Point<Protocol, Format> getPointFor( String identifier, String schemaName, Protocol protocol, Format format )
    {
    if( idPointMap.containsKey( identifier ) )
      return idPointMap.get( identifier );

    Point<Protocol, Format> point = createPointFor( schemaName, identifier, protocol, format );

    idPointMap.put( identifier, point );

    return point;
    }

  private Point<Protocol, Format> createPointFor( String schemaName, String identifier, Protocol protocol, Format format )
    {
    if( protocol == null )
      protocol = getDefaultProtocolFor( schemaName, identifier );

    if( format == null )
      format = getDefaultFormatFor( schemaName, identifier );

    return new Point<Protocol, Format>( protocol, format );
    }

  public Protocol getDefaultProtocolFor( String schemaName, String identifier )
    {
    TableDef table = rootSchemaDef.findTableFor( identifier );

    if( table != null && table.getProtocol() != null )
      return table.getActualProtocol();

    SchemaDef schemaDef = getSchemaDef( schemaName );

    Protocol protocol = ProtocolProperties.findProtocolFor( schemaDef, identifier );

    if( protocol == null && schemaDef != null )
      protocol = schemaDef.findDefaultProtocol();

    return protocol;
    }

  public Format getDefaultFormatFor( String schemaName, String identifier )
    {
    TableDef tableDef = rootSchemaDef.findTableFor( identifier );

    // return declared format by given table
    if( tableDef != null && tableDef.getFormat() != null )
      return tableDef.getActualFormat();

    SchemaDef schemaDef = getSchemaDef( schemaName );

    Format format = FormatProperties.findFormatFor( schemaDef, identifier );

    if( format == null && schemaDef != null )
      format = schemaDef.findDefaultFormat();

    return format;
    }

  public Collection<String> getStereotypeNames()
    {
    return rootSchemaDef.getStereotypeNames();
    }

  public Collection<String> getStereotypeNames( String schemaName )
    {
    SchemaDef schema = getSchemaDef( schemaName );

    if( schema == null )
      throw new IllegalArgumentException( "schema does not exist: " + schemaName );

    return schema.getStereotypeNames();
    }

  private Stereotype<Protocol, Format> findStereotype( SchemaDef schema, String stereotypeName )
    {
    if( schema == null )
      return null;

    Stereotype<Protocol, Format> stereotype = schema.getStereotype( stereotypeName );

    if( stereotype != null )
      return stereotype;

    return findStereotype( schema.getParentSchema(), stereotypeName );
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

  public TableDef findTableDefFor( String identifier )
    {
    return rootSchemaDef.findTableFor( identifier );
    }

  public Stereotype<Protocol, Format> findStereotypeFor( String identifier )
    {
    TableDef tableDef = findTableDefFor( identifier );

    if( tableDef == null )
      return null;

    return tableDef.getStereotype();
    }

  public boolean createStereotype( String schemaName, String name, Fields fields )
    {
    SchemaDef schemaDef = getSchemaDef( schemaName );

    return createStereotype( schemaDef, name, fields, null ) != null;
    }

  private Stereotype<Protocol, Format> createStereotype( SchemaDef schemaDef, String name, Fields fields, Protocol protocol )
    {
    Protocol defaultProtocol = protocol == null ? schemaDef.findDefaultProtocol() : protocol;

    Stereotype<Protocol, Format> stereotype = new Stereotype<Protocol, Format>( getFormatHandlers(), defaultProtocol, name, fields );

    schemaDef.addStereotype( stereotype );

    return stereotype;
    }

  public Stereotype getStereoTypeFor( Fields fields )
    {
    return rootSchemaDef.findStereotypeFor( fields );
    }

  public Stereotype getStereoTypeFor( String schemaName, Fields fields )
    {
    SchemaDef schema = getSchemaDef( schemaName );

    if( schema == null )
      throw new IllegalArgumentException( "schema does not exist: " + schemaName );

    return schema.findStereotypeFor( fields );
    }

  public Fields getFieldsFor( String identifier )
    {
    String name = platformBroker.createTableNameFrom( identifier );

    if( nameFieldsMap.containsKey( name ) )
      return nameFieldsMap.get( name );

    Point<Protocol, Format> point = getPointFor( identifier, null, null, null );

    Resource<Protocol, Format, SinkMode> resource = new Resource<Protocol, Format, SinkMode>( identifier, point.protocol, point.format, SinkMode.KEEP );

    Tap tap = createTapFor( rootSchemaDef.findStereotypeFor( Fields.UNKNOWN ), resource );

    if( !resourceExists( tap ) )
      return null;

    Fields fields = tap.retrieveSourceFields( platformBroker.getFlowProcess() );

    nameFieldsMap.put( name, fields );

    return fields;
    }

  private boolean resourceExists( Tap tap )
    {
    if( tap == null )
      return false;

    try
      {
      return tap.resourceExists( platformBroker.getFlowProcess().getConfigCopy() );
      }
    catch( IOException exception )
      {
      return false;
      }
    }

  public Tap createTapFor( String identifier, SinkMode sinkMode )
    {
    TableDef tableDef = findTableDefFor( identifier );

    if( tableDef == null )
      throw new IllegalArgumentException( "no table for identifier: " + identifier );

    return createTapFor( tableDef, sinkMode );
    }

  public Tap createTapFor( TableDef tableDef, SinkMode sinkMode )
    {
    Protocol protocol = tableDef.getActualProtocol();

    ProtocolHandler<Protocol, Format> protocolHandler = getProtocolHandlers().findHandlerFor( protocol );

    if( protocolHandler == null )
      throw new IllegalArgumentException( "no protocol handler for protocol: " + protocol );

    Resource<Protocol, Format, SinkMode> resource = tableDef.getResourceWith( sinkMode );

    return protocolHandler.createTap( tableDef.getStereotype(), resource );
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

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;

    if( !( object instanceof SchemaCatalog ) )
      return false;

    SchemaCatalog catalog = (SchemaCatalog) object;

    if( formatHandlers != null ? !formatHandlers.equals( catalog.formatHandlers ) : catalog.formatHandlers != null )
      return false;
    if( idPointMap != null ? !idPointMap.equals( catalog.idPointMap ) : catalog.idPointMap != null )
      return false;
    if( nameFieldsMap != null ? !nameFieldsMap.equals( catalog.nameFieldsMap ) : catalog.nameFieldsMap != null )
      return false;
    if( protocolHandlers != null ? !protocolHandlers.equals( catalog.protocolHandlers ) : catalog.protocolHandlers != null )
      return false;
    if( rootSchemaDef != null ? !rootSchemaDef.equals( catalog.rootSchemaDef ) : catalog.rootSchemaDef != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = ( rootSchemaDef != null ? rootSchemaDef.hashCode() : 0 );
    result = 31 * result + ( protocolHandlers != null ? protocolHandlers.hashCode() : 0 );
    result = 31 * result + ( formatHandlers != null ? formatHandlers.hashCode() : 0 );
    result = 31 * result + ( nameFieldsMap != null ? nameFieldsMap.hashCode() : 0 );
    result = 31 * result + ( idPointMap != null ? idPointMap.hashCode() : 0 );
    return result;
    }
  }
