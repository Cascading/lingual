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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import cascading.bind.catalog.Stereotype;
import cascading.bind.catalog.Stereotypes;
import cascading.lingual.util.InsensitiveMap;
import cascading.lingual.util.MultiProperties;
import cascading.tuple.Fields;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyList;

/** Class SchemaDef manages all "schema" related meta-data. */
@JsonAutoDetect(
  fieldVisibility = JsonAutoDetect.Visibility.ANY,
  getterVisibility = JsonAutoDetect.Visibility.NONE,
  setterVisibility = JsonAutoDetect.Visibility.NONE,
  isGetterVisibility = JsonAutoDetect.Visibility.NONE
)
public class SchemaDef extends Def
  {
  private static final Logger LOG = LoggerFactory.getLogger( SchemaDef.class );

  @JsonProperty
  private Protocol defaultProtocol;

  @JsonProperty
  private Format defaultFormat;

  @JsonProperty
  private final MultiProperties<Protocol> protocolProperties = new MultiProperties<Protocol>();

  @JsonProperty
  private final MultiProperties<Format> formatProperties = new MultiProperties<Format>();

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private final Stereotypes<Protocol, Format> stereotypes = new Stereotypes<Protocol, Format>();

  @JsonProperty
  @JsonManagedReference
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private final InsensitiveMap<SchemaDef> childSchemas = new InsensitiveMap<SchemaDef>();

  @JsonProperty
  @JsonManagedReference
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private final InsensitiveMap<TableDef> childTables = new InsensitiveMap<TableDef>();

  @JsonProperty
  @JsonManagedReference
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private final InsensitiveMap<ProviderDef> providers = new InsensitiveMap<ProviderDef>();

  public SchemaDef()
    {
    }

  public SchemaDef( Protocol defaultProtocol, Format defaultFormat )
    {
    this.defaultProtocol = defaultProtocol;
    this.defaultFormat = defaultFormat;
    }

  public SchemaDef( SchemaDef parentSchema, String name, Protocol defaultProtocol, Format defaultFormat )
    {
    super( parentSchema, name );
    this.defaultProtocol = defaultProtocol;
    this.defaultFormat = defaultFormat;
    }

  public SchemaDef( SchemaDef parentSchema, String name, Protocol defaultProtocol, Format defaultFormat, String identifier )
    {
    super( parentSchema, name, identifier );
    this.defaultProtocol = defaultProtocol;
    this.defaultFormat = defaultFormat;
    }

  public SchemaDef copyWith( String name )
    {
    return new SchemaDef( parentSchema, name, defaultProtocol, defaultFormat, identifier );
    }

  public boolean isRoot()
    {
    return getParentSchema() == null;
    }

  public Protocol getDefaultProtocol()
    {
    return defaultProtocol;
    }

  public Protocol findDefaultProtocol()
    {
    if( getDefaultProtocol() != null )
      return getDefaultProtocol();

    return getParentSchema().findDefaultProtocol();
    }

  public void setDefaultProtocol( Protocol defaultProtocol )
    {
    this.defaultProtocol = defaultProtocol;
    }

  public Format getDefaultFormat()
    {
    return defaultFormat;
    }

  public Format findDefaultFormat()
    {
    if( getDefaultFormat() != null )
      return getDefaultFormat();

    return getParentSchema().findDefaultFormat();
    }

  public void setDefaultFormat( Format defaultFormat )
    {
    this.defaultFormat = defaultFormat;
    }

  public void addProtocolProperties( Protocol protocol, Map<String, List<String>> properties )
    {
    protocolProperties.putProperties( protocol, properties );
    }

  public void addProtocolProperty( Protocol protocol, String property, String... values )
    {
    protocolProperties.addProperty( protocol, property, values );
    }

  public void addProtocolProperty( Protocol protocol, String property, List<String> values )
    {
    protocolProperties.addProperty( protocol, property, values );
    }

  public Map<String, List<String>> removeProtocolProperties( Protocol protocol )
    {
    return protocolProperties.removeRow( protocol );
    }

  public List<String> getProtocolProperty( Protocol protocol, String property )
    {
    List<String> result = findAllProtocolProperties( protocol ).get( property );

    if( result == null )
      return emptyList();

    return result;
    }

  public Map<String, Map<String, List<String>>> findProviderProtocolProperties( Protocol protocol )
    {
    Map<String, Map<String, List<String>>> foundProviders = new HashMap<String, Map<String, List<String>>>();

    // parent first, schemes can override providers by name
    if( !isRoot() )
      foundProviders.putAll( getParentSchema().findProviderProtocolProperties( protocol ) );

    for( Map.Entry<String, ProviderDef> entry : providers.entrySet() )
      {
      Map<Protocol, Map<String, List<String>>> properties = entry.getValue().getProtocolProperties();

      if( properties.containsKey( protocol ) )
        foundProviders.put( entry.getKey(), properties.get( protocol ) );
      }

    return foundProviders;
    }

  public Map<String, List<String>> findAllProtocolProperties( Protocol protocol )
    {
    Map<String, List<String>> schemaProperties = findProtocolProperties( protocol );
    Map<String, Map<String, List<String>>> providerProperties = findProviderProtocolProperties( protocol );
    List<String> providerNames = schemaProperties.get( FormatProperties.PROVIDER );
    Map<String, List<String>> allProperties = new LinkedHashMap<String, List<String>>();

    if( providerNames == null || providerNames.isEmpty() )
      {
      LOG.info( "no provider set for protocol: " + protocol + ", in schema: " + getName() );

      if( providerProperties.keySet().size() == 0 )
        throw new IllegalStateException( "protocol: " + protocol + " not available from provider" );

      if( providerProperties.keySet().size() > 1 )
        throw new IllegalStateException( "for protocol: " + protocol + ", found multiple providers: [" + Joiner.on( ',' ).join( providerProperties.keySet() ) + "]" );

      LOG.info( "using sole provider default properties: " + providerProperties.keySet().iterator().next() );

      allProperties.putAll( providerProperties.values().iterator().next() );
      }
    else
      {
      for( String providerName : providerNames )
        {
        if( providerProperties.containsKey( providerName ) )
          {
          LOG.info( "using provider default properties: " + providerName );

          allProperties.putAll( providerProperties.get( providerName ) );
          break;
          }
        }
      }

    allProperties.putAll( schemaProperties );

    return allProperties;
    }

  protected Map<String, List<String>> findProtocolProperties( Protocol protocol )
    {
    Map<String, List<String>> result = new LinkedHashMap<String, List<String>>();

    if( !isRoot() )
      result.putAll( getParentSchema().getProtocolProperties( protocol ) );

    result.putAll( getProtocolProperties( protocol ) );

    return result;
    }

  private Map<String, List<String>> getProtocolProperties( Protocol protocol )
    {
    Map<String, List<String>> result = new LinkedHashMap<String, List<String>>();

    if( protocolProperties.getValueFor( protocol ) != null )
      result.putAll( protocolProperties.getValueFor( protocol ) );

    return result;
    }

  public Map<Protocol, List<String>> findPropertyByProtocols( String property )
    {
    Map<Protocol, List<String>> values = new HashMap<Protocol, List<String>>();

    findProtocolProperty( values, property );

    return values;
    }

  private void findProtocolProperty( Map<Protocol, List<String>> values, String property )
    {
    if( !isRoot() )
      getParentSchema().findProtocolProperty( values, property );

    for( ProviderDef providerDef : getProviderDefs() )
      {
      MultiProperties<Protocol> providerProperties = MultiProperties.create( providerDef.getProtocolProperties() );

      values.putAll( providerProperties.getKeyFor( property ) );
      }

    values.putAll( protocolProperties.getKeyFor( property ) );
    }

  public void addFormatProperties( Format format, Map<String, List<String>> properties )
    {
    formatProperties.putProperties( format, properties );
    }

  public void addFormatProperty( Format format, String property, String... values )
    {
    formatProperties.addProperty( format, property, values );
    }

  public void addFormatProperty( Format format, String property, List<String> values )
    {
    formatProperties.addProperty( format, property, values );
    }

  public Map<String, List<String>> removeFormatProperties( Format format )
    {
    return formatProperties.removeRow( format );
    }

  public List<String> getFormatProperty( Format format, String property )
    {
    List<String> result = findAllFormatProperties( format ).get( property );

    if( result == null )
      return emptyList();

    return result;
    }

  public Map<String, Map<String, List<String>>> findProviderFormatProperties( Format format )
    {
    Map<String, Map<String, List<String>>> foundProviders = new HashMap<String, Map<String, List<String>>>();

    // parent first, schemes can override providers by name
    if( !isRoot() )
      foundProviders.putAll( getParentSchema().findProviderFormatProperties( format ) );

    for( Map.Entry<String, ProviderDef> entry : providers.entrySet() )
      {
      Map<Format, Map<String, List<String>>> properties = entry.getValue().getFormatProperties();

      if( properties.containsKey( format ) )
        foundProviders.put( entry.getKey(), properties.get( format ) );
      }

    return foundProviders;
    }

  public Map<String, List<String>> findAllFormatProperties( Format format )
    {
    Map<String, List<String>> schemaProperties = findFormatProperties( format );
    Map<String, Map<String, List<String>>> providerProperties = findProviderFormatProperties( format );
    List<String> providerNames = schemaProperties.get( FormatProperties.PROVIDER );
    Map<String, List<String>> allProperties = new LinkedHashMap<String, List<String>>();

    if( providerNames == null || providerNames.isEmpty() )
      {
      LOG.info( "no provider set for format: " + format + ", in schema: " + getName() );

      if( providerProperties.keySet().size() == 0 )
        throw new IllegalStateException( "format: " + format + " not available from provider" );

      if( providerProperties.keySet().size() > 1 )
        throw new IllegalStateException( "for format: " + format + ", found multiple providers: [" + Joiner.on( ',' ).join( providerProperties.keySet() ) + "]" );

      LOG.info( "using sole provider default properties: " + providerProperties.keySet().iterator().next() );

      allProperties.putAll( providerProperties.values().iterator().next() );
      }
    else
      {
      for( String providerName : providerNames )
        {
        if( providerProperties.containsKey( providerName ) )
          {
          LOG.info( "using provider default properties: " + providerName );

          allProperties.putAll( providerProperties.get( providerName ) );
          break;
          }
        }
      }

    allProperties.putAll( schemaProperties );

    return allProperties;
    }

  // format properties, excluding defaults from providers
  protected Map<String, List<String>> findFormatProperties( Format format )
    {
    Map<String, List<String>> result = new LinkedHashMap<String, List<String>>();

    if( !isRoot() )
      result.putAll( getParentSchema().getFormatProperties( format ) );

    result.putAll( getFormatProperties( format ) );

    return result;
    }

  private Map<String, List<String>> getFormatProperties( Format format )
    {
    Map<String, List<String>> result = new LinkedHashMap<String, List<String>>();

    if( formatProperties.getValueFor( format ) != null )
      result.putAll( formatProperties.getValueFor( format ) );

    return result;
    }

  public Map<Format, List<String>> findPropertyByFormats( String property )
    {
    Map<Format, List<String>> values = new HashMap<Format, List<String>>();

    findFormatProperty( values, property );

    return values;
    }

  private void findFormatProperty( Map<Format, List<String>> values, String property )
    {
    if( !isRoot() )
      getParentSchema().findFormatProperty( values, property );

    for( ProviderDef providerDef : getProviderDefs() )
      {
      MultiProperties<Format> providerProperties = MultiProperties.create( providerDef.getFormatProperties() );

      values.putAll( providerProperties.getKeyFor( property ) );
      }

    values.putAll( formatProperties.getKeyFor( property ) );
    }

  public Collection<SchemaDef> getChildSchemas()
    {
    return childSchemas.values();
    }

  public Collection<String> getChildSchemaNames()
    {
    return getDefNames( childSchemas );
    }

  public Collection<TableDef> getChildTables()
    {
    return childTables.values();
    }

  public Collection<String> getChildTableNames()
    {
    return getDefNames( childTables );
    }

  public boolean addSchema( String name, Protocol protocol, Format format )
    {
    if( childSchemas.containsKey( name ) )
      return false;

    childSchemas.put( name, new SchemaDef( this, name, protocol, format ) );

    return true;
    }

  public boolean addSchema( String name, Protocol protocol, Format format, String identifier )
    {
    if( childSchemas.containsKey( name ) )
      return false;

    childSchemas.put( name, new SchemaDef( this, name, protocol, format, identifier ) );

    return true;
    }

  public boolean removeSchema( String schemaName )
    {
    return childSchemas.remove( schemaName ) != null;
    }

  public boolean renameSchema( String schemaName, String newName )
    {
    SchemaDef schemaDef = childSchemas.get( schemaName );

    if( schemaDef == null )
      return false;

    schemaDef.setName( newName );
    childSchemas.remove( schemaName );
    childSchemas.put( newName, schemaDef );

    return true;
    }

  public SchemaDef getSchema( String schemaName )
    {
    return childSchemas.get( schemaName );
    }

  public TableDef getTable( String tableName )
    {
    return childTables.get( tableName );
    }

  public TableDef getTableChecked( String tableName )
    {
    TableDef tableDef = getTable( tableName );

    if( tableDef == null )
      throw new IllegalArgumentException( "table does not exist: " + tableName + ", in schema: " + getName() );

    return tableDef;
    }

  public boolean removeTable( String schemaName, String tableName )
    {
    SchemaDef schemaDef = getSchema( schemaName );

    if( schemaDef == null )
      return false;

    return schemaDef.removeTable( tableName );
    }

  private boolean removeTable( String tableName )
    {
    return childTables.remove( tableName ) != null;
    }

  public boolean renameTable( String schemaName, String tableName, String newName )
    {
    SchemaDef schemaDef = getSchema( schemaName );

    if( schemaDef == null )
      return false;

    return schemaDef.renameTable( tableName, newName );
    }

  private boolean renameTable( String tableName, String newName )
    {
    TableDef tableDef = childTables.get( tableName );

    if( tableDef == null )
      return false;

    tableDef.setName( newName );
    childTables.remove( tableName );
    childTables.put( newName, tableDef );

    return true;
    }

  public void addTable( String name, String identifier, Stereotype<Protocol, Format> stereotype, Protocol protocol, Format format )
    {
    if( childTables.containsKey( name ) )
      throw new IllegalArgumentException( "table named: " + childTables.get( name ).getName() + " already exists in schema: " + getName() );

    LOG.debug( "adding table: {}, to schema: {}", name, getName() );

    childTables.put( name, new TableDef( this, name, identifier, stereotype, protocol, format ) );
    }

  public TableDef findTableFor( String identifier )
    {
    for( TableDef tableDef : childTables.values() )
      {
      if( tableDef.getIdentifier().equalsIgnoreCase( identifier ) )
        return tableDef;
      }

    for( SchemaDef schemaDef : childSchemas.values() )
      {
      TableDef tableDef = schemaDef.findTableFor( identifier );

      if( tableDef != null )
        return tableDef;
      }

    return null;
    }

  public Stereotype<Protocol, Format> getStereotypeChecked( String stereotypeName )
    {
    Stereotype<Protocol, Format> stereotype = getStereotype( stereotypeName );

    if( stereotype == null )
      throw new IllegalArgumentException( "stereotype does not exist: " + stereotype + ", in schema: " + getName() );

    return stereotype;
    }

  public Stereotype<Protocol, Format> getStereotype( String stereotypeName )
    {
    if( stereotypeName == null || stereotypeName.isEmpty() )
      return null;

    return stereotypes.getStereotypeFor( stereotypeName );
    }

  public void addStereotype( Stereotype<Protocol, Format> stereotype )
    {
    stereotypes.addStereotype( stereotype );
    }

  public boolean removeStereotype( String name )
    {
    return stereotypes.removeStereotype( name );
    }

  public boolean renameStereotype( String name, String newName )
    {
    return stereotypes.renameStereotype( name, newName );
    }

  public Stereotype<Protocol, Format> findStereotypeFor( String stereotypeName )
    {
    Stereotype<Protocol, Format> stereotype = stereotypes.getStereotypeFor( stereotypeName );

    if( stereotype != null || isRoot() )
      return stereotype;

    return getParentSchema().findStereotypeFor( stereotypeName );
    }

  public Stereotype<Protocol, Format> findStereotypeFor( Fields fields )
    {
    Stereotype<Protocol, Format> stereotype = stereotypes.getStereotypeFor( fields );

    if( stereotype != null || isRoot() )
      return stereotype;

    return getParentSchema().findStereotypeFor( fields );
    }

  public boolean hasStereotype( String name )
    {
    if( name == null )
      return false;

    return stereotypes.getStereotypeFor( name ) != null;
    }

  public Collection<Protocol> getSchemaDefinedProtocols()
    {
    Set<Protocol> protocols = new HashSet<Protocol>();

    for( ProviderDef providerDef : providers.values() )
      protocols.addAll( providerDef.getProtocolProperties().keySet() );

    protocols.addAll( protocolProperties.getKeys() );

    return protocols;
    }

  public Collection<Protocol> getAllProtocols()
    {
    Collection<Protocol> protocols = getSchemaDefinedProtocols();

    if( !isRoot() )
      protocols.addAll( getParentSchema().getAllProtocols() );

    return protocols;
    }

  public Collection<Format> getSchemaDefinedFormats()
    {
    Set<Format> formats = new HashSet<Format>();

    for( ProviderDef providerDef : providers.values() )
      formats.addAll( providerDef.getFormatProperties().keySet() );

    formats.addAll( formatProperties.getKeys() );

    return formats;
    }

  public Collection<Format> getAllFormats()
    {
    Collection<Format> formats = getSchemaDefinedFormats();

    if( !isRoot() )
      formats.addAll( getParentSchema().getAllFormats() );

    return formats;
    }

  public Collection<String> getAllFormatNames()
    {
    Set<String> names = new TreeSet<String>();

    for( Format format : getAllFormats() )
      names.add( format.toString() );

    return names;
    }

  public Collection<String> getAllProtocolNames()
    {
    Set<String> names = new TreeSet<String>();

    for( Protocol protocol : getAllProtocols() )
      names.add( protocol.toString() );

    return names;
    }

  public Collection<String> getStereotypeNames()
    {
    return stereotypes.getStereotypeNames();
    }

  public Collection<String> getProviderNames()
    {
    return providers.keySet();
    }

  public boolean removeProviderDef( String providerDefName )
    {
    providers.remove( providerDefName );
    return true;
    }

  public boolean renameProviderDef( String oldProviderDefName, String newProviderDefName )
    {
    ProviderDef oldProviderDef = providers.get( oldProviderDefName );
    ProviderDef newProviderDef = new ProviderDef( this, newProviderDefName, oldProviderDef.getIdentifier(), oldProviderDef.getProperties() );

    providers.remove( oldProviderDef.getName() );
    providers.put( newProviderDefName, newProviderDef );
    return true;
    }

  public void addProviderDef( String name, String jarName, Map<String, String> properties, String md5Hash )
    {
    addProviderDef( new ProviderDef( this, name, jarName, properties, md5Hash ) );
    }

  public void addProviderDef( ProviderDef providerDef )
    {
    if( providers.containsKey( providerDef.getName() ) )
      throw new IllegalArgumentException( "provider named: " + providerDef.getName() + " already exists in schema: " + getName() );

    LOG.debug( "adding provider: {}, to schema: {}", providerDef.getName(), getName() );

    providers.put( providerDef.getName(), providerDef );
    }

  public List<ProviderDef> getProviderDefs()
    {
    return new ArrayList<ProviderDef>( providers.values() );
    }

  public List<ProviderDef> getAllProviderDefs()
    {
    Set<ProviderDef> providerDefs = new LinkedHashSet<ProviderDef>( providers.values() );

    if( !isRoot() )
      providerDefs.addAll( getParentSchema().getAllProviderDefs() );

    return new ArrayList<ProviderDef>( providerDefs );
    }

  public Map<String, ProviderDef> getAllProviderDefsMap()
    {
    // top down, topmost take precedence
    Map<String, ProviderDef> providerDefs = new LinkedHashMap<String, ProviderDef>( providers );

    if( !isRoot() )
      {
      Map<String, ProviderDef> parentMap = getParentSchema().getAllProviderDefsMap();

      for( String name : parentMap.keySet() )
        {
        if( !providerDefs.containsKey( name ) )
          providerDefs.put( name, parentMap.get( name ) );
        }
      }

    return providerDefs;
    }

  public ProviderDef getProviderDef( String providerDefName )
    {
    return providers.get( providerDefName );
    }

  public ProviderDef findProviderDefFor( String providerName )
    {
    if( getProviderDef( providerName ) != null )
      return getProviderDef( providerName );

    if( !isRoot() )
      return getParentSchema().findProviderDefFor( providerName );

    return null;
    }

  public ProviderDef getProtocolProvider( Protocol protocol )
    {
    Map<Protocol, List<String>> map = findPropertyByProtocols( FormatProperties.PROVIDER );
    List<String> providers = map.get( protocol );

    if( providers.isEmpty() )
      return null;

    String providerName = providers.get( 0 );

    return findProviderDefFor( providerName );
    }

  public ProviderDef getFormatProvider( Format format )
    {
    Map<Format, List<String>> map = findPropertyByFormats( FormatProperties.PROVIDER );
    List<String> providers = map.get( format );

    if( providers.isEmpty() )
      return null;

    String providerName = providers.get( 0 );

    return findProviderDefFor( providerName );
    }

  public ProviderDef findProviderDefFor( Protocol protocol )
    {
    List<String> providers = findAllProtocolProperties( protocol ).get( SchemaProperties.PROVIDER );

    for( String providerName : providers )
      {
      ProviderDef providerDef = findProviderDefFor( providerName );

      if( providerDef != null )
        return providerDef;
      }

    return null;
    }

  public ProviderDef findProviderDefFor( Format format )
    {
    List<String> providers = findAllFormatProperties( format ).get( SchemaProperties.PROVIDER );

    if( providers == null )
      return null;

    for( String providerName : providers )
      {
      ProviderDef providerDef = findProviderDefFor( providerName );

      if( providerDef != null )
        return providerDef;
      }

    return null;
    }

  public Collection<String> getDefNames( InsensitiveMap<? extends Def> defInsensitiveMap )
    {
    Function<Def, String> defNameExtractor =
      new Function<Def, String>()
      {
      public String apply( Def def )
        {
        return def.getName();
        }
      };
    Collection<? extends Def> values = defInsensitiveMap.values();
    return Collections2.transform( values, defNameExtractor );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof SchemaDef ) )
      return false;
    if( !super.equals( object ) )
      return false;

    SchemaDef schemaDef = (SchemaDef) object;

    if( childSchemas != null ? !childSchemas.equals( schemaDef.childSchemas ) : schemaDef.childSchemas != null )
      return false;
    if( childTables != null ? !childTables.equals( schemaDef.childTables ) : schemaDef.childTables != null )
      return false;
    if( formatProperties != null ? !formatProperties.equals( schemaDef.formatProperties ) : schemaDef.formatProperties != null )
      return false;
    if( protocolProperties != null ? !protocolProperties.equals( schemaDef.protocolProperties ) : schemaDef.protocolProperties != null )
      return false;
    if( stereotypes != null ? !stereotypes.equals( schemaDef.stereotypes ) : schemaDef.stereotypes != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( stereotypes != null ? stereotypes.hashCode() : 0 );
    result = 31 * result + ( childSchemas != null ? childSchemas.hashCode() : 0 );
    result = 31 * result + ( childTables != null ? childTables.hashCode() : 0 );
    result = 31 * result + ( protocolProperties != null ? protocolProperties.hashCode() : 0 );
    result = 31 * result + ( formatProperties != null ? formatProperties.hashCode() : 0 );
    return result;
    }
  }
