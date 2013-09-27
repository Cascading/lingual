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

package cascading.lingual.catalog.service;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import cascading.bind.catalog.Stereotype;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.FormatProperties;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.ProtocolProperties;
import cascading.lingual.catalog.ProviderDef;
import cascading.lingual.catalog.Repo;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.SchemaDef;
import cascading.lingual.catalog.TableDef;
import cascading.lingual.util.InsensitiveMap;
import cascading.tuple.Fields;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
@JsonAutoDetect(
  fieldVisibility = JsonAutoDetect.Visibility.ANY,
  getterVisibility = JsonAutoDetect.Visibility.NONE,
  setterVisibility = JsonAutoDetect.Visibility.NONE,
  isGetterVisibility = JsonAutoDetect.Visibility.NONE
)
public class JSONSchemaCatalog implements Serializable, SchemaCatalog
  {
  private String platformName;

  @JsonProperty
  private final SchemaDef rootSchemaDef;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private final InsensitiveMap<Repo> repositories = new InsensitiveMap<Repo>();

  protected JSONSchemaCatalog()
    {
    this.rootSchemaDef = new SchemaDef();
    }

  public JSONSchemaCatalog( String platformName, Protocol defaultProtocol, Format defaultFormat )
    {
    this.platformName = platformName;
    this.rootSchemaDef = new SchemaDef( defaultProtocol, defaultFormat );
    }

  public String getPlatformName()
    {
    return platformName;
    }

  @Override
  public Protocol getDefaultProtocol()
    {
    return rootSchemaDef.getDefaultProtocol();
    }

  @Override
  public Format getDefaultFormat()
    {
    return rootSchemaDef.getDefaultFormat();
    }

  @Override
  public SchemaDef getRootSchemaDef()
    {
    return rootSchemaDef;
    }

  @Override
  public boolean schemaExists( String schemaName )
    {
    if( schemaName == null ) // root schema
      return true;

    return getSchemaNames().contains( schemaName );
    }

  @Override
  public Collection<String> getSchemaNames()
    {
    return getRootSchemaDef().getChildSchemaNames();
    }

  @Override
  public SchemaDef getSchemaDef( String schemaName )
    {
    if( schemaName == null )
      return getRootSchemaDef();

    return getRootSchemaDef().getSchema( schemaName );
    }

  @Override
  public boolean addSchemaDef( String name, Protocol protocol, Format format, String identifier )
    {
    return getRootSchemaDef().addSchema( name, protocol, format, identifier );
    }

  @Override
  public boolean removeSchemaDef( String schemaName )
    {
    return getRootSchemaDef().removeSchema( schemaName );
    }

  @Override
  public boolean renameSchemaDef( String schemaName, String newName )
    {
    return getRootSchemaDef().renameSchema( schemaName, newName );
    }

  SchemaDef getSchemaDefChecked( String schemaName )
    {
    SchemaDef schemaDef = getSchemaDef( schemaName );

    if( schemaDef == null )
      throw new IllegalArgumentException( "schema does not exist: " + schemaName );

    return schemaDef;
    }

  @Override
  public Collection<String> getTableNames( String schemaName )
    {
    return getSchemaDefChecked( schemaName ).getChildTableNames();
    }

  @Override
  public TableDef getTableDef( String schemaName, String tableName )
    {
    return getSchemaDef( schemaName ).getTable( tableName );
    }

  @Override
  public boolean removeTableDef( String schemaName, String tableName )
    {
    return getRootSchemaDef().removeTable( schemaName, tableName );
    }

  @Override
  public boolean renameTableDef( String schemaName, String tableName, String renameName )
    {
    return getRootSchemaDef().renameTable( schemaName, tableName, renameName );
    }

  @Override
  public Collection<String> getFormatNames( String schemaName )
    {
    return getSchemaDefChecked( schemaName ).getAllFormatNames();
    }

  @Override
  public List<String> getFormatProperty( String schemeName, Format format, String propertyName )
    {
    return getSchemaDef( schemeName ).getFormatProperty( format, propertyName );
    }

  @Override
  public Collection<String> getProtocolNames( String schemaName )
    {
    return getSchemaDefChecked( schemaName ).getAllProtocolNames();
    }

  @Override
  public List<String> getProtocolProperty( String schemeName, Protocol protocol, String propertyName )
    {
    return getSchemaDef( schemeName ).getProtocolProperty( protocol, propertyName );
    }

  @Override
  public Collection<String> getProviderNames( String schemaName )
    {
    return getSchemaDef( schemaName ).getProviderNames();
    }

  @Override
  public void addProviderDef( String schemaName, String name, String jarName, Map<String, String> propertyMap, String md5Hash )
    {
    getSchemaDef( schemaName ).addProviderDef( name, jarName, propertyMap, md5Hash );
    }

  @Override
  public ProviderDef getProviderDef( String schemaName, String providerName )
    {
    return getSchemaDef( schemaName ).getProviderDef( providerName );
    }

  @Override
  public void addTableDef( String schemaName, String name, String identifier, Stereotype<Protocol, Format> stereotype, Protocol protocol, Format format )
    {
    getSchemaDef( schemaName ).addTable( name, identifier, stereotype, protocol, format );
    }

  @Override
  public ProviderDef findProviderDefFor( String schemaName, Format format )
    {
    return getSchemaDef( schemaName ).findProviderDefFor( format );
    }

  @Override
  public ProviderDef findProviderDefFor( String schemaName, Protocol protocol )
    {
    return getSchemaDef( schemaName ).findProviderDefFor( protocol );
    }

  @Override
  public ProviderDef findProviderFor( String schemaName, String providerName )
    {
    return getSchemaDef( schemaName ).findProviderDefFor( providerName );
    }

  @Override
  public boolean removeProviderDef( String schemaName, String providerName )
    {
    return getSchemaDef( schemaName ).removeProviderDef( providerName );
    }

  @Override
  public boolean renameProviderDef( String schemaName, String oldProviderName, String newProviderName )
    {
    return getSchemaDef( schemaName ).renameProviderDef( oldProviderName, newProviderName );
    }

  @Override
  public Collection<String> getRepositoryNames()
    {
    return repositories.keySet();
    }

  @Override
  public Collection<Repo> getRepositories()
    {
    return repositories.values();
    }

  @Override
  public Repo getRepository( String repoName )
    {
    return repositories.get( repoName );
    }

  @Override
  public void addRepository( Repo repo )
    {
    repositories.put( repo.getRepoName(), repo );
    }

  @Override
  public void removeRepository( String repoName )
    {
    repositories.remove( repoName );
    }

  @Override
  public boolean renameRepository( String oldName, String newName )
    {
    Repo oldRepo = repositories.get( oldName );

    repositories.remove( oldName );

    Repo newRepo = new Repo( newName, oldRepo.getRepoUrl() );
    repositories.put( newName, newRepo );

    return true;
    }

  @Override
  public Protocol getDefaultProtocolFor( String schemaName, String identifier )
    {
    return getDefaultProtocolFor( getSchemaDef( schemaName ), identifier );
    }

  protected Protocol getDefaultProtocolFor( SchemaDef schemaDef, String identifier )
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

  @Override
  public Collection<Protocol> getSchemaDefinedProtocols( String schemaName )
    {
    return getSchemaDefChecked( schemaName ).getSchemaDefinedProtocols();
    }

  @Override
  public Format getDefaultFormatFor( String schemaName, String identifier )
    {
    return getDefaultFormatFor( getSchemaDef( schemaName ), identifier );
    }

  protected Format getDefaultFormatFor( SchemaDef schemaDef, String identifier )
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

  @Override
  public Collection<Format> getSchemaDefinedFormats( String schemaName )
    {
    return getSchemaDefChecked( schemaName ).getSchemaDefinedFormats();
    }

  @Override
  public Collection<String> getStereotypeNames()
    {
    return rootSchemaDef.getStereotypeNames();
    }

  @Override
  public Collection<String> getStereotypeNames( String schemaName )
    {
    return getSchemaDefChecked( schemaName ).getStereotypeNames();
    }

  @Override
  public Stereotype<Protocol, Format> getStereotype( String schemaName, String stereotypeName )
    {
    return getSchemaDef( schemaName ).getStereotype( stereotypeName );
    }

  @Override
  public Stereotype<Protocol, Format> findStereotype( SchemaDef schemaDef, String stereotypeName )
    {
    if( schemaDef == null )
      return null;

    Stereotype<Protocol, Format> stereotype = schemaDef.getStereotype( stereotypeName );

    if( stereotype != null )
      return stereotype;

    return findStereotype( schemaDef.getParentSchema(), stereotypeName );
    }

  @Override
  public boolean removeStereotype( String schemaName, String stereotypeName )
    {
    return getSchemaDefChecked( schemaName ).removeStereotype( stereotypeName );
    }

  @Override
  public boolean renameStereotype( String schemaName, String name, String newName )
    {
    return getSchemaDefChecked( schemaName ).renameStereotype( name, newName );
    }

  @Override
  public boolean createStereotype( String schemaName, String name, Fields fields )
    {
    SchemaDef schemaDef = getSchemaDefChecked( schemaName );

    return createStereotype( schemaDef, name, fields ) != null;
    }

  protected Stereotype<Protocol, Format> createStereotype( SchemaDef schemaDef, String name, Fields fields )
    {
    Stereotype<Protocol, Format> stereotype = new Stereotype<Protocol, Format>( name, fields );

    schemaDef.addStereotype( stereotype );

    return stereotype;
    }

  @Override
  public Stereotype getStereoTypeFor( Fields fields )
    {
    return rootSchemaDef.findStereotypeFor( fields );
    }

  @Override
  public Stereotype getStereoTypeFor( String schemaName, Fields fields )
    {
    return getSchemaDefChecked( schemaName ).findStereotypeFor( fields );
    }

  @Override
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

  @Override
  public boolean removeFormat( String schemaName, Format format )
    {
    SchemaDef schemaDef = getSchemaDefChecked( schemaName );

    schemaDef.removeFormatProperties( format );

    return true;
    }

  @Override
  public boolean renameFormat( String schemaName, Format oldFormat, Format newFormat )
    {
    SchemaDef schemaDef = getSchemaDefChecked( schemaName );

    Map<String, List<String>> oldProperties = schemaDef.removeFormatProperties( oldFormat );
    schemaDef.addFormatProperties( newFormat, oldProperties );

    return true;
    }

  @Override
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

  @Override
  public boolean removeProtocol( String schemaName, Protocol protocol )
    {
    SchemaDef schemaDef = getSchemaDefChecked( schemaName );

    schemaDef.removeProtocolProperties( protocol );

    return true;
    }

  @Override
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

    JSONSchemaCatalog that = (JSONSchemaCatalog) object;

    if( repositories != null ? !repositories.equals( that.repositories ) : that.repositories != null )
      return false;

    if( rootSchemaDef != null ? !rootSchemaDef.equals( that.rootSchemaDef ) : that.rootSchemaDef != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = repositories != null ? repositories.hashCode() : 0;
    result = 31 * result + ( rootSchemaDef != null ? rootSchemaDef.hashCode() : 0 );
    return result;
    }
  }
