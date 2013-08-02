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
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import cascading.bind.catalog.Point;
import cascading.bind.catalog.Resource;
import cascading.bind.catalog.Stereotype;
import cascading.bind.catalog.handler.FormatHandler;
import cascading.bind.catalog.handler.FormatHandlers;
import cascading.bind.catalog.handler.ProtocolHandler;
import cascading.bind.catalog.handler.ProtocolHandlers;
import cascading.lingual.catalog.provider.ProviderDefinition;
import cascading.lingual.jdbc.LingualConnection;
import cascading.lingual.platform.LingualFormatHandler;
import cascading.lingual.platform.LingualProtocolHandler;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.tap.TapSchema;
import cascading.lingual.util.InsensitiveMap;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.hydromatic.optiq.impl.java.MapSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.lingual.catalog.provider.ProviderDefinition.getProviderDefinitions;

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
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private final InsensitiveMap<Repo> repositories = new InsensitiveMap<Repo>();

  @JsonProperty
  private SchemaDef rootSchemaDef;

  @JsonProperty
  private Map<String, Fields> nameFieldsMap = new InsensitiveMap<Fields>();

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

  public PlatformBroker getPlatformBroker()
    {
    return platformBroker;
    }

  public void initializeNew()
    {
    registerDefaultRepositories();
    registerDefaultProviders();

    if( !rootSchemaDef.hasStereotype( "UNKNOWN" ) )
      createStereotype( rootSchemaDef, "UNKNOWN", Fields.UNKNOWN );
    }

  private void registerDefaultProviders()
    {
    for( ProviderDefinition providerDefinition : getDefaultProviderProperties() )
      {
      Map<String, String> properties = providerDefinition.getProperties();

      rootSchemaDef.addProviderDef( providerDefinition.getProviderName(), null, properties, null );
      }
    }

  private void registerDefaultRepositories()
    {
    addRepo( Repo.MAVEN_CENTRAL );
    addRepo( Repo.MAVEN_LOCAL );
    addRepo( Repo.MAVEN_CONJARS );
    }

  protected List<ProviderDefinition> getDefaultProviderProperties()
    {
    List<ProviderDefinition> properties = new ArrayList<ProviderDefinition>();

    try
      {
      Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources( ProviderDefinition.CASCADING_BIND_PROVIDER_PROPERTIES );

      while( resources.hasMoreElements() )
        {
        URL url = resources.nextElement();

        LOG.debug( "loading properties from: {}", url );

        Properties definitions = new Properties();

        InputStream inputStream = url.openStream();

        definitions.load( inputStream );

        inputStream.close();

        Collections.addAll( properties, getProviderDefinitions( definitions ) );
        }
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to load default provider properties", exception );
      }

    return properties;
    }

  public ProtocolHandlers<Protocol, Format> getProtocolHandlers( Def def )
    {
    return new ProtocolHandlers<Protocol, Format>( createProtocolHandlers( def ) );
    }

  private List<ProtocolHandler<Protocol, Format>> createProtocolHandlers( Def def )
    {
    if( def instanceof TableDef )
      return createProtocolHandlers( (TableDef) def );

    return createProtocolHandlers( ( (SchemaDef) def ) );
    }

  public FormatHandlers<Protocol, Format> getFormatHandlersFor( Def def )
    {
    return new FormatHandlers<Protocol, Format>( createFormatHandlers( def ) );
    }

  private List<FormatHandler<Protocol, Format>> createFormatHandlers( Def def )
    {
    if( def instanceof TableDef )
      return createFormatHandlers( (TableDef) def );

    return createFormatHandlers( ( (SchemaDef) def ) );
    }

  public SchemaDef getRootSchemaDef()
    {
    return rootSchemaDef;
    }

  public TableDef resolveTableDef( String[] names )
    {
    if( names == null )
      throw new IllegalArgumentException( "names array may not be null" );

    if( names.length == 0 )
      return null;

    SchemaDef current = getRootSchemaDef();

    for( int i = 0; i < names.length - 1; i++ )
      {
      if( current == null )
        throw new IllegalArgumentException( "could not find table def at: " + Arrays.toString( names ) + " on: " + names[ i ] );

      current = current.getSchema( names[ i ] );
      }

    return current.getTable( names[ names.length - 1 ] );
    }

  public Collection<String> getSchemaNames()
    {
    return getRootSchemaDef().getChildSchemaNames();
    }

  public SchemaDef getSchemaDef( String schemaName )
    {
    if( schemaName == null )
      return getRootSchemaDef();

    return getRootSchemaDef().getSchema( schemaName );
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
    return getSchemaDefChecked( schemaName ).getChildTableNames();
    }

  public SchemaDef getSchemaDefChecked( String schemaName )
    {
    SchemaDef schemaDef = getSchemaDef( schemaName );

    if( schemaDef == null )
      throw new IllegalArgumentException( "schema does not exist: " + schemaName );

    return schemaDef;
    }

  public void createTableDefFor( String identifier )
    {
    createTableDefFor( getRootSchemaDef(), null, identifier, null, null, null, null );
    }

  public void createTableDefFor( String schemaName, String tableName, String tableIdentifier, Fields fields, String protocolName, String formatName )
    {
    Point<Protocol, Format> point = getPointFor( tableIdentifier, schemaName, Protocol.getProtocol( protocolName ), Format.getFormat( formatName ) );

    tableIdentifier = getFullPath( tableIdentifier );

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
    SchemaDef schemaDef = getSchemaDefChecked( schemaName );

    return createTableDefFor( schemaDef, tableName, identifier, stereotypeName, null, protocol, format );
    }

  protected String createTableDefFor( SchemaDef schemaDef, String tableName, String tableIdentifier, String stereotypeName, Fields fields, Protocol protocol, Format format )
    {
    Point<Protocol, Format> point = getPointFor( tableIdentifier, schemaDef.getName(), protocol, format );

    if( protocol == null )
      tableIdentifier = getFullPath( tableIdentifier );

    LOG.debug( "using table path: {}", tableIdentifier );

    if( tableName == null )
      tableName = platformBroker.createTableNameFrom( tableIdentifier );

    Stereotype<Protocol, Format> stereotype = null;

    if( stereotypeName != null )
      stereotype = findStereotype( schemaDef, stereotypeName );

    if( stereotype == null )
      stereotype = findOrCreateStereotype( schemaDef, fields, tableIdentifier );

    if( stereotype == null )
      throw new IllegalArgumentException( "stereotype does not exist: " + stereotypeName );

    schemaDef.addTable( tableName, tableIdentifier, stereotype, point.protocol, point.format );

    return tableName;
    }

  private String getFullPath( String identifier )
    {
    if( platformBroker == null )
      return identifier;

    if( URI.create( identifier ).getScheme() != null )
      return identifier;

    return platformBroker.getFullPath( identifier );
    }

  private Stereotype<Protocol, Format> findOrCreateStereotype( SchemaDef schema, Fields fields, String identifier )
    {
    Stereotype<Protocol, Format> stereotype = findStereotypeFor( identifier );

    if( stereotype != null )
      return stereotype;

    if( fields == null )
      fields = getFieldsFor( schema, identifier );

    if( fields == null )
      return schema.findStereotypeFor( Fields.UNKNOWN );

    String stereotypeName = platformBroker.createTableNameFrom( identifier );

    stereotype = schema.findStereotypeFor( fields );

    if( stereotype == null )
      stereotype = createStereotype( schema, stereotypeName, fields );

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


        String childSchemaDescription;
        if( currentSchemaDef.getIdentifier() != null )
          childSchemaDescription = String.format( "'%s' ( %s )", childSchemaDef.getName(), currentSchemaDef.getIdentifier() );
        else
          childSchemaDescription = String.format( "'%s'", childSchemaDef.getName() );

        String name = currentSchemaDef.getName() == null ? "root" : currentSchemaDef.getName();

        LOG.info( "added schema: {}, to: '{}'", childSchemaDescription, name );
        }

      childSchema.addTapTablesFor( childSchemaDef );

      addSchemas( connection, childSchema, childSchemaDef );
      }
    }

  public Collection<String> getFormatNames( String schemaName )
    {
    return getSchemaDefChecked( schemaName ).getAllFormatNames();
    }

  public List<String> getFormatProperty( String schemeName, String format, String propertyName )
    {
    return getFormatProperty( schemeName, Format.getFormat( format ), propertyName );
    }

  public List<String> getFormatProperty( String schemeName, Format format, String propertyName )
    {
    return getSchemaDef( schemeName ).getFormatProperty( format, propertyName );
    }

  public Collection<String> getProtocolNames( String schemaName )
    {
    return getSchemaDefChecked( schemaName ).getProtocolNames();
    }

  public List<String> getProtocolProperty( String schemeName, String protocol, String propertyName )
    {
    return getProtocolProperty( schemeName, Protocol.getProtocol( protocol ), propertyName );
    }

  public List<String> getProtocolProperty( String schemeName, Protocol protocol, String propertyName )
    {
    return getSchemaDef( schemeName ).getProtocolProperty( protocol, propertyName );
    }

  public Collection<String> getProviderNames()
    {
    return rootSchemaDef.getProviderNames();
    }

  public Collection<String> getProviderNames( String schemaName )
    {
    return getSchemaDefChecked( schemaName ).getProviderNames();
    }

  public void addProviderDef( String schemaName, ProviderDef providerDef )
    {
    getSchemaDefChecked( schemaName ).addProviderDef( providerDef );
    }

  public ProviderDef findProviderDefFor( String schemaName, Format format )
    {
    return getSchemaDefChecked( schemaName ).findProviderDefFor( format );
    }

  public ProviderDef findProviderDefFor( String schemaName, Protocol protocol )
    {
    return getSchemaDefChecked( schemaName ).findProviderDefFor( protocol );
    }

  public ProviderDef findProviderFor( String schemaName, String providerName )
    {
    return getSchemaDefChecked( schemaName ).findProviderDefFor( providerName );
    }

  public boolean removeProviderDef( String schemaName, String providerName )
    {
    return getSchemaDefChecked( schemaName ).removeProviderDef( providerName );
    }

  public boolean renameProviderDef( String schemaName, String oldProviderName, String newProviderName )
    {
    return getSchemaDefChecked( schemaName ).renameProviderDef( oldProviderName, newProviderName );
    }

  public Collection<String> getMavenRepoNames()
    {
    return repositories.keySet();
    }

  public Collection<Repo> getRepositories()
    {
    return repositories.values();
    }

  public Repo getMavenRepo( String repoName )
    {
    return repositories.get( repoName );
    }

  public void addRepo( Repo repo )
    {
    repositories.put( repo.getRepoName(), repo );
    }

  public void removeMavenRepo( String repoName )
    {
    repositories.remove( repoName );
    }

  public boolean renameMavenRepo( String oldName, String newName )
    {
    Repo oldRepo = repositories.get( oldName );

    repositories.remove( oldName );

    Repo newRepo = new Repo( newName, oldRepo.getRepoUrl() );
    repositories.put( newName, newRepo );

    return true;
    }

  protected Point<Protocol, Format> getPointFor( String identifier, String schemaName, Protocol protocol, Format format )
    {
    if( protocol == null )
      protocol = getDefaultProtocolFor( schemaName, identifier );

    if( !getSchemaDef( schemaName ).getAllProtocols().contains( protocol ) )
      throw new IllegalStateException( "no protocol found named: " + protocol );

    if( format == null )
      format = getDefaultFormatFor( schemaName, identifier );

    if( !getSchemaDef( schemaName ).getAllFormats().contains( format ) )
      throw new IllegalStateException( "no format found named: " + format );

    return new Point<Protocol, Format>( protocol, format );
    }

  public Protocol getDefaultProtocolFor( String schemaName, String identifier )
    {
    return getDefaultProtocolFor( getSchemaDef( schemaName ), identifier );
    }

  public Protocol getDefaultProtocolFor( SchemaDef schemaDef, String identifier )
    {
    // not using root by default in case identifier is registered with multiple tables
    TableDef table = schemaDef.findTableFor( identifier );

    if( table != null && table.getProtocol() != null )
      return table.getActualProtocol();

    Protocol protocol = ProtocolProperties.findProtocolFor( schemaDef, identifier );

    if( protocol == null )
      protocol = schemaDef.findDefaultProtocol();

    return protocol;
    }

  public Format getDefaultFormatFor( String schemaName, String identifier )
    {
    return getDefaultFormatFor( getSchemaDef( schemaName ), identifier );
    }

  public Format getDefaultFormatFor( SchemaDef schemaDef, String identifier )
    {
    // not using root by default in case identifier is registered with multiple tables
    TableDef tableDef = schemaDef.findTableFor( identifier );

    // return declared format by given table
    if( tableDef != null && tableDef.getFormat() != null )
      return tableDef.getActualFormat();

    Format format = FormatProperties.findFormatFor( schemaDef, identifier );

    if( format == null )
      format = schemaDef.findDefaultFormat();

    return format;
    }

  public Collection<String> getStereotypeNames()
    {
    return rootSchemaDef.getStereotypeNames();
    }

  public Collection<String> getStereotypeNames( String schemaName )
    {
    return getSchemaDefChecked( schemaName ).getStereotypeNames();
    }

  private Stereotype<Protocol, Format> findStereotype( SchemaDef schemaDef, String stereotypeName )
    {
    if( schemaDef == null )
      return null;

    Stereotype<Protocol, Format> stereotype = schemaDef.getStereotype( stereotypeName );

    if( stereotype != null )
      return stereotype;

    return findStereotype( schemaDef.getParentSchema(), stereotypeName );
    }

  public boolean removeStereotype( String schemaName, String stereotypeName )
    {
    return getSchemaDefChecked( schemaName ).removeStereotype( stereotypeName );
    }

  public boolean renameStereotype( String schemaName, String name, String newName )
    {

    return getSchemaDefChecked( schemaName ).renameStereotype( name, newName );
    }

  public TableDef findTableDefFor( String identifier )
    {
    return rootSchemaDef.findTableFor( identifier );
    }

  public Stereotype<Protocol, Format> findStereotypeFor( String identifier )
    {
    TableDef tableDef = findTableDefFor( identifier ); // could be more than one

    if( tableDef == null )
      return null;

    return tableDef.getStereotype();
    }

  public boolean createStereotype( String schemaName, String name, Fields fields )
    {
    SchemaDef schemaDef = getSchemaDefChecked( schemaName );

    return createStereotype( schemaDef, name, fields ) != null;
    }

  private Stereotype<Protocol, Format> createStereotype( SchemaDef schemaDef, String name, Fields fields )
    {
    Stereotype<Protocol, Format> stereotype = new Stereotype<Protocol, Format>( name, fields );

    schemaDef.addStereotype( stereotype );

    return stereotype;
    }

  public Stereotype getStereoTypeFor( Fields fields )
    {
    return rootSchemaDef.findStereotypeFor( fields );
    }

  public Stereotype getStereoTypeFor( String schemaName, Fields fields )
    {
    return getSchemaDefChecked( schemaName ).findStereotypeFor( fields );
    }

  public Fields getFieldsFor( SchemaDef schemaDef, String identifier )
    {
    String name = platformBroker.createTableNameFrom( identifier );

    if( nameFieldsMap.containsKey( name ) )
      return nameFieldsMap.get( name );

    Point<Protocol, Format> point = getPointFor( identifier, schemaDef.getName(), null, null );

    Resource<Protocol, Format, SinkMode> resource = new Resource<Protocol, Format, SinkMode>( schemaDef.getName(), identifier, point.protocol, point.format, SinkMode.KEEP );

    Tap tap = createTapFor( schemaDef, schemaDef.findStereotypeFor( Fields.UNKNOWN ), resource );

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
    Format format = tableDef.getActualFormat();

    ProtocolHandler<Protocol, Format> protocolHandler = getProtocolHandlers( tableDef ).findHandlerFor( protocol );
    FormatHandler<Protocol, Format> formatHandler = getFormatHandlersFor( tableDef ).findHandlerFor( protocol, format );

    if( protocolHandler == null )
      throw new IllegalArgumentException( "no protocol handler for protocol: " + protocol );

    if( formatHandler == null )
      throw new IllegalArgumentException( "no format handler for format: " + format );

    Scheme scheme = formatHandler.createScheme( tableDef.getStereotype(), protocol, format );

    Resource<Protocol, Format, SinkMode> resource = tableDef.getResourceWith( sinkMode );

    return protocolHandler.createTap( scheme, resource );
    }

  private Tap createTapFor( SchemaDef schemaDef, Stereotype<Protocol, Format> stereotype, Resource<Protocol, Format, SinkMode> resource )
    {
    ProtocolHandler<Protocol, Format> protocolHandler = getProtocolHandlers( schemaDef ).findHandlerFor( resource.getProtocol() );
    FormatHandler<Protocol, Format> formatHandler = getFormatHandlersFor( schemaDef ).findHandlerFor( resource.getProtocol(), resource.getFormat() );

    if( protocolHandler == null || formatHandler == null )
      return null;

    Scheme scheme = formatHandler.createScheme( stereotype, resource.getProtocol(), resource.getFormat() );

    return protocolHandler.createTap( scheme, resource );
    }

  public Resource<Protocol, Format, SinkMode> getResourceFor( String identifier, SinkMode mode )
    {
    Point<Protocol, Format> point = getPointFor( identifier, null, null, null );

    Protocol protocol = point.protocol;
    Format format = point.format;

    return new Resource<Protocol, Format, SinkMode>( identifier, protocol, format, mode );
    }

  public Resource<Protocol, Format, SinkMode> getResourceFor( TableDef tableDef, SinkMode mode )
    {
    Protocol protocol = tableDef.getActualProtocol();
    Format format = tableDef.getActualFormat();

    return new Resource<Protocol, Format, SinkMode>( tableDef.getParentSchema().getName(), tableDef.identifier, protocol, format, mode );
    }

  protected List<ProtocolHandler<Protocol, Format>> createProtocolHandlers( TableDef tableDef )
    {
    Protocol protocol = tableDef.getActualProtocol();
    ProviderDef providerDef = tableDef.getParentSchema().findProviderDefFor( protocol );
    ProtocolHandler<Protocol, Format> handler = createProtocolHandler( providerDef );

    Map<String, List<String>> properties = tableDef.getParentSchema().findAllProtocolProperties( protocol );

    ( (LingualProtocolHandler) handler ).addProperties( protocol, properties );

    return Arrays.asList( handler );
    }

  protected List<ProtocolHandler<Protocol, Format>> createProtocolHandlers( SchemaDef schemaDef )
    {
    Map<String, ProtocolHandler<Protocol, Format>> handlers = new HashMap<String, ProtocolHandler<Protocol, Format>>();
    Collection<Protocol> protocols = Arrays.asList( schemaDef.findDefaultProtocol() );

    for( Protocol protocol : protocols )
      {
      ProviderDef providerDef = schemaDef.findProviderDefFor( protocol );
      ProtocolHandler protocolHandler = handlers.get( providerDef.getName() );

      if( protocolHandler == null )
        {
        protocolHandler = createProtocolHandler( providerDef );
        handlers.put( providerDef.getName(), protocolHandler );
        }

      Map<String, List<String>> protocolProperties = schemaDef.findAllProtocolProperties( protocol );

      ( (LingualProtocolHandler) protocolHandler ).addProperties( protocol, protocolProperties );
      }

    return new ArrayList<ProtocolHandler<Protocol, Format>>( handlers.values() );
    }

  protected List<FormatHandler<Protocol, Format>> createFormatHandlers( TableDef tableDef )
    {
    Format format = tableDef.getActualFormat();
    ProviderDef providerDef = tableDef.getParentSchema().findProviderDefFor( format );
    FormatHandler<Protocol, Format> handler = createFormatHandler( providerDef );

    Map<String, List<String>> properties = tableDef.getParentSchema().findAllFormatProperties( format );

    ( (LingualFormatHandler) handler ).addProperties( format, properties );

    return Arrays.asList( handler );
    }

  protected List<FormatHandler<Protocol, Format>> createFormatHandlers( SchemaDef schemaDef )
    {
    Map<String, FormatHandler<Protocol, Format>> handlers = new HashMap<String, FormatHandler<Protocol, Format>>();
    Collection<Format> formats = Arrays.asList( schemaDef.findDefaultFormat() );

    for( Format format : formats )
      {
      ProviderDef providerDef = schemaDef.findProviderDefFor( format );
      FormatHandler formatHandler = handlers.get( providerDef.getName() );

      if( formatHandler == null )
        {
        formatHandler = createFormatHandler( providerDef );
        handlers.put( providerDef.getName(), formatHandler );
        }

      Map<String, List<String>> formatProperties = schemaDef.findAllFormatProperties( format );

      ( (LingualFormatHandler) formatHandler ).addProperties( format, formatProperties );
      }

    return new ArrayList<FormatHandler<Protocol, Format>>( handlers.values() );
    }

  protected abstract ProtocolHandler<Protocol, Format> createProtocolHandler( ProviderDef providerDef );

  protected abstract FormatHandler<Protocol, Format> createFormatHandler( ProviderDef providerDef );

  public void addUpdateFormat( String schemaName, Format format, List<String> extensions, Map<String, String> properties, String providerName )
    {
    SchemaDef schemaDef = getSchemaDefChecked( schemaName );

    if( extensions != null && !extensions.isEmpty() )
      schemaDef.addFormatProperty( format, FormatProperties.EXTENSIONS, extensions );

    if( providerName != null )
      schemaDef.addFormatProperty( format, FormatProperties.PROVIDER, providerName );

    if( properties == null )
      return;

    for( Map.Entry<String, String> entry : properties.entrySet() )
      schemaDef.addFormatProperty( format, entry.getKey(), entry.getValue() );
    }

  public boolean removeFormat( String schemaName, Format format )
    {
    SchemaDef schemaDef = getSchemaDefChecked( schemaName );

    schemaDef.removeFormatProperties( format );

    return true;
    }

  public boolean renameFormat( String schemaName, Format oldFormat, Format newFormat )
    {
    SchemaDef schemaDef = getSchemaDefChecked( schemaName );

    Map<String, List<String>> oldProperties = schemaDef.removeFormatProperties( oldFormat );
    schemaDef.addFormatProperties( newFormat, oldProperties );

    return true;
    }

  public void addUpdateProtocol( String schemaName, Protocol protocol, List<String> schemes, Map<String, String> properties, String providerName )
    {
    SchemaDef schemaDef = getSchemaDefChecked( schemaName );

    if( schemes != null && !schemes.isEmpty() )
      schemaDef.addProtocolProperty( protocol, ProtocolProperties.SCHEMES, schemes );

    if( providerName != null )
      schemaDef.addProtocolProperty( protocol, ProtocolProperties.PROVIDER, providerName );

    if( properties == null )
      return;

    for( Map.Entry<String, String> entry : properties.entrySet() )
      schemaDef.addProtocolProperty( protocol, entry.getKey(), entry.getValue() );
    }

  public boolean removeProtocol( String schemaName, Protocol protocol )
    {
    SchemaDef schemaDef = getSchemaDefChecked( schemaName );

    schemaDef.removeProtocolProperties( protocol );

    return true;
    }

  public boolean renameProtocol( String schemaName, Protocol oldProtocol, Protocol newProtocol )
    {
    SchemaDef schemaDef = getSchemaDefChecked( schemaName );

    Map<String, List<String>> oldProperties = schemaDef.removeProtocolProperties( oldProtocol );
    schemaDef.addProtocolProperties( newProtocol, oldProperties );

    return true;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    SchemaCatalog that = (SchemaCatalog) object;

    if( repositories != null ? !repositories.equals( that.repositories ) : that.repositories != null )
      return false;
    if( nameFieldsMap != null ? !nameFieldsMap.equals( that.nameFieldsMap ) : that.nameFieldsMap != null )
      return false;
    if( platformBroker != null ? !platformBroker.equals( that.platformBroker ) : that.platformBroker != null )
      return false;
    if( rootSchemaDef != null ? !rootSchemaDef.equals( that.rootSchemaDef ) : that.rootSchemaDef != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = platformBroker != null ? platformBroker.hashCode() : 0;
    result = 31 * result + ( repositories != null ? repositories.hashCode() : 0 );
    result = 31 * result + ( rootSchemaDef != null ? rootSchemaDef.hashCode() : 0 );
    result = 31 * result + ( nameFieldsMap != null ? nameFieldsMap.hashCode() : 0 );
    return result;
    }
  }
