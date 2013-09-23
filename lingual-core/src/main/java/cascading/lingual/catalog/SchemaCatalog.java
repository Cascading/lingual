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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import cascading.bind.catalog.Stereotype;
import cascading.tuple.Fields;

/**
 * SchemaCatalog defines the CRUD like behaviors for interacting with a collection of schemas, tables, and supporting
 * types.
 */
public interface SchemaCatalog
  {
  // Schema

  SchemaDef getRootSchemaDef();

  Collection<String> getSchemaNames();

  SchemaDef getSchemaDef( String schemaName );

  boolean addSchemaDef( String name, Protocol protocol, Format format, String identifier );

  boolean removeSchemaDef( String schemaName );

  boolean renameSchemaDef( String schemaName, String newName );

  boolean schemaExists( String schemaName );

  // Table

  Collection<String> getTableNames( String schemaName );

  TableDef getTableDef( String schemaName, String tableName );

  void addTableDef( String schemaName, String name, String identifier, Stereotype<Protocol, Format> stereotype, Protocol protocol, Format format );

  boolean removeTableDef( String schemaName, String tableName );

  boolean renameTableDef( String schemaName, String tableName, String renameName );

  // Protocol

  Protocol getDefaultProtocol();

  Protocol getDefaultProtocolFor( String schemaName, String identifier );

  Collection<String> getProtocolNames( String schemaName );

  List<String> getProtocolProperty( String schemeName, Protocol protocol, String propertyName );

  void addUpdateProtocol( String schemaName, Protocol protocol, List<String> schemes, Map<String, String> properties, String providerName );

  boolean removeProtocol( String schemaName, Protocol protocol );

  boolean renameProtocol( String schemaName, Protocol oldProtocol, Protocol newProtocol );

  Collection<Protocol> getSchemaDefinedProtocols( String schemaName );

  // Format

  Format getDefaultFormat();

  Collection<String> getFormatNames( String schemaName );

  Format getDefaultFormatFor( String schemaName, String identifier );

  List<String> getFormatProperty( String schemeName, Format format, String propertyName );

  void addUpdateFormat( String schemaName, Format format, List<String> extensions, Map<String, String> properties, String providerName );

  boolean removeFormat( String schemaName, Format format );

  boolean renameFormat( String schemaName, Format oldFormat, Format newFormat );

  Collection<Format> getSchemaDefinedFormats( String schemaName );

  // Provider

  Collection<String> getProviderNames( String schemaName );

  void addProviderDef( String schemaName, String name, String jarName, Map<String, String> propertyMap, String md5Hash );

  ProviderDef getProviderDef( String schemaName, String providerName );

  ProviderDef findProviderDefFor( String schemaName, Format format );

  ProviderDef findProviderDefFor( String schemaName, Protocol protocol );

  ProviderDef findProviderFor( String schemaName, String providerName );

  boolean removeProviderDef( String schemaName, String providerName );

  boolean renameProviderDef( String schemaName, String oldProviderName, String newProviderName );

  // Stereotype

  Collection<String> getStereotypeNames();

  Collection<String> getStereotypeNames( String schemaName );

  Stereotype<Protocol, Format> getStereotype( String schemaName, String stereotypeName );

  Stereotype<Protocol, Format> findStereotype( SchemaDef schemaDef, String stereotypeName );

  boolean createStereotype( String schemaName, String name, Fields fields );

  boolean removeStereotype( String schemaName, String stereotypeName );

  boolean renameStereotype( String schemaName, String name, String newName );

  Stereotype getStereoTypeFor( Fields fields );

  Stereotype getStereoTypeFor( String schemaName, Fields fields );

  // Repository

  Collection<String> getRepositoryNames();

  Collection<Repo> getRepositories();

  Repo getRepository( String repoName );

  void addRepository( Repo repo );

  void removeRepository( String repoName );

  boolean renameRepository( String oldName, String newName );
  }