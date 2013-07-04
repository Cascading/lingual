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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public List<String> getProtocolProperty( Protocol protocol, String property )
    {
    List<String> result = findProtocolProperties( protocol ).get( property );

    if( result == null )
      return Collections.EMPTY_LIST;

    return result;
    }

  public Map<String, List<String>> findProtocolProperties( Protocol protocol )
    {
    Map<String, List<String>> result = new HashMap<String, List<String>>();

    if( !isRoot() )
      result.putAll( getParentSchema().findProtocolProperties( protocol ) );

    if( protocolProperties.getValueFor( protocol ) != null )
      result.putAll( protocolProperties.getValueFor( protocol ) );

    return result;
    }

  public Map<Protocol, List<String>> getPropertyByProtocols( String property )
    {
    Map<Protocol, List<String>> values = new HashMap<Protocol, List<String>>();

    populateProtocolProperty( values, property );

    return values;
    }

  private void populateProtocolProperty( Map<Protocol, List<String>> values, String property )
    {
    if( !isRoot() )
      getParentSchema().populateProtocolProperty( values, property );

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

  public List<String> getFormatProperty( Format format, String property )
    {
    List<String> result = findFormatProperties( format ).get( property );

    if( result == null )
      return Collections.EMPTY_LIST;

    return result;
    }

  public Map<String, List<String>> findFormatProperties( Format format )
    {
    Map<String, List<String>> result = new HashMap<String, List<String>>();

    if( !isRoot() )
      result.putAll( getParentSchema().findFormatProperties( format ) );

    if( formatProperties.getValueFor( format ) != null )
      result.putAll( formatProperties.getValueFor( format ) );

    return result;
    }

  public Map<Format, List<String>> getPropertyByFormats( String property )
    {
    Map<Format, List<String>> values = new HashMap<Format, List<String>>();

    populateFormatProperty( values, property );

    return values;
    }

  private void populateFormatProperty( Map<Format, List<String>> values, String property )
    {
    if( !isRoot() )
      getParentSchema().populateFormatProperty( values, property );

    values.putAll( formatProperties.getKeyFor( property ) );
    }

  protected void registerProperties( ProviderDef providerDef )
    {
    Map<Format, Map<String, List<String>>> formats = providerDef.getFormatProperties();

    for( Format format : formats.keySet() )
      addFormatProperties( format, formats.get( format ) );

    Map<Protocol, Map<String, List<String>>> protocols = providerDef.getProtocolProperties();

    for( Protocol protocol : protocols.keySet() )
      addProtocolProperties( protocol, protocols.get( protocol ) );
    }

  public Collection<SchemaDef> getChildSchemas()
    {
    return childSchemas.values();
    }

  public Collection<String> getChildSchemaNames()
    {
    return childSchemas.keySet();
    }

  public Collection<TableDef> getChildTables()
    {
    return childTables.values();
    }

  public Collection<String> getChildTableNames()
    {
    return childTables.keySet();
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
    SchemaDef schemaDef = childSchemas.remove( schemaName );

    if( schemaDef == null )
      return false;

    childSchemas.put( newName, schemaDef.copyWith( newName ) );

    return true;
    }

  public SchemaDef getSchema( String name )
    {
    return childSchemas.get( name );
    }

  public TableDef getTable( String name )
    {
    return childTables.get( name );
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
    TableDef tableDef = childTables.remove( tableName );

    if( tableDef == null )
      return false;

    childTables.put( newName, tableDef.copyWith( newName ) );

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

  public Collection<Protocol> getAllProtocols()
    {
    Set<Protocol> protocols = new HashSet<Protocol>();

    protocols.addAll( protocolProperties.getKeys() );

    if( !isRoot() )
      protocols.addAll( getParentSchema().getAllProtocols() );

    return protocols;
    }

  public Collection<Format> getAllFormats()
    {
    Set<Format> formats = new HashSet<Format>();

    formats.addAll( formatProperties.getKeys() );

    if( !isRoot() )
      formats.addAll( getParentSchema().getAllFormats() );

    return formats;
    }

  public Collection<String> getAllFormatNames()
    {
    Set<String> names = new TreeSet<String>();

    for( ProviderDef providerDef : getAllProviderDefs() )
      {
      for( Format format : providerDef.getFormatProperties().keySet() )
        names.add( format.toString() );
      }

    return names;
    }

  public Collection<String> getProtocolNames()
    {
    Set<String> names = new TreeSet<String>();

    for( ProviderDef providerDef : getAllProviderDefs() )
      {
      for( Protocol protocol : providerDef.getProtocolProperties().keySet() )
        names.add( protocol.toString() );
      }

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
    return providers.remove( providerDefName ) != null;
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

    registerProperties( providerDef );
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

  public ProviderDef findProviderDefFor( Protocol protocol )
    {
    List<String> providers = findProtocolProperties( protocol ).get( SchemaProperties.PROVIDER );

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
    List<String> providers = findFormatProperties( format ).get( SchemaProperties.PROVIDER );

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
