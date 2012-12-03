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

package cascading.lingual.platform;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.planner.PlatformInfo;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.json.JsonFactory;
import cascading.lingual.optiq.meta.Branch;
import cascading.tap.type.FileType;
import cascading.util.Util;
import com.fasterxml.jackson.databind.ObjectMapper;

import static cascading.lingual.jdbc.Driver.*;

/**
 *
 */
public abstract class PlatformBroker<Config>
  {
  public static final String META_DATA_PATH_PROP = "lingual.meta-data.path";
  public static final String META_DATA_PATH = ".lingual";

  public static final String CATALOG_FILE_PROP = "lingual.catalog.name";
  public static final String CATALOG_FILE = "catalog";

  private Properties properties;
  private SchemaCatalog catalog;

  protected PlatformBroker()
    {
    }

  public void setProperties( Properties properties )
    {
    this.properties = properties;
    }

  public Properties getProperties()
    {
    return properties;
    }

  public abstract String getName();

  public abstract Config getConfig();

  public abstract FlowProcess<Config> getFlowProcess();

  public SchemaCatalog getCatalog()
    {
    if( catalog == null )
      catalog = loadCatalog();

    return catalog;
    }

  public boolean initializeMetaData()
    {
    String path = getFullMetadataPath();

    if( pathExists( path ) )
      return true;

    if( !createPath( path ) )
      throw new RuntimeException( "unable to create catalog: " + path );

    return false;
    }

  public void writeCatalog()
    {
    String catalogPath = getFullCatalogPath();

    OutputStream outputStream = getOutputStream( catalogPath );

    if( true )
      writeAsObjectAndClose( catalogPath, outputStream );
    else
      writeAsJsonAndClose( catalogPath, outputStream );
    }

  public String getFullMetadataPath()
    {
    String catalogPath = getStringProperty( CATALOG_PROP );

    return makeFullMetadataFilePath( catalogPath );
    }

  public String getFullCatalogPath()
    {
    String catalogPath = getStringProperty( CATALOG_PROP );

    return makeFullCatalogFilePath( catalogPath );
    }

  private void writeAsObjectAndClose( String catalogPath, OutputStream outputStream )
    {
    try
      {
      ObjectOutputStream objectOutputStream = new ObjectOutputStream( outputStream );

      objectOutputStream.writeObject( getCatalog() );

      objectOutputStream.close();
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to write path: " + catalogPath, exception );
      }
    }

  private void writeAsJsonAndClose( String catalogPath, OutputStream outputStream )
    {
    ObjectMapper mapper = getObjectMapper();

    try
      {
      mapper.writeValue( outputStream, getCatalog() );

      outputStream.close();
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to write path: " + catalogPath, exception );
      }
    }

  private SchemaCatalog loadCatalog()
    {
    catalog = readCatalog();

    if( catalog == null )
      catalog = newInstance();

    if( properties.containsKey( SCHEMAS_PROP ) )
      loadSchemas( catalog );

    if( properties.containsKey( TABLES_PROP ) )
      loadTables( catalog );

    return catalog;
    }

  private SchemaCatalog readCatalog()
    {
    String catalogPath = getFullCatalogPath();

    InputStream inputStream = getInputStream( catalogPath );

    if( inputStream == null )
      return null;

    if( true )
      return readAsObjectAndClose( catalogPath, inputStream );
    else
      return readAsJsonAndClose( catalogPath, inputStream );
    }

  private SchemaCatalog readAsObjectAndClose( String catalogPath, InputStream inputStream )
    {
    try
      {
      ObjectInputStream objectInputStream = new ObjectInputStream( inputStream );
      SchemaCatalog schemaCatalog = (SchemaCatalog) objectInputStream.readObject();

      objectInputStream.close();

      return schemaCatalog;
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to read path: " + catalogPath, exception );
      }
    catch( ClassNotFoundException exception )
      {
      throw new RuntimeException( "unable to read path: " + catalogPath, exception );
      }
    }

  private SchemaCatalog readAsJsonAndClose( String catalogPath, InputStream inputStream )
    {
    ObjectMapper mapper = getObjectMapper();

    try
      {
      SchemaCatalog schemaCatalog = mapper.readValue( inputStream, getCatalogClass() );

      inputStream.close();

      schemaCatalog.setPlatformBroker( this );

      return schemaCatalog;
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to read path: " + catalogPath, exception );
      }
    }

  private ObjectMapper getObjectMapper()
    {
    return JsonFactory.getObjectMapper( this );
    }

  private String makeFullMetadataFilePath( String catalogPath )
    {
    String metaDataPath = properties.getProperty( META_DATA_PATH_PROP, META_DATA_PATH );

    return makePath( getFileSeparator(), catalogPath, metaDataPath );
    }

  private String makeFullCatalogFilePath( String catalogPath )
    {
    String metaDataPath = properties.getProperty( META_DATA_PATH_PROP, META_DATA_PATH );
    String metaDataFile = properties.getProperty( CATALOG_FILE_PROP, CATALOG_FILE );

    return makePath( getFileSeparator(), catalogPath, metaDataPath, metaDataFile );
    }

  public static String makePath( String fileSeparator, String rootPath, String... elements )
    {
    if( rootPath == null )
      rootPath = ".";

    if( !rootPath.endsWith( fileSeparator ) )
      rootPath += fileSeparator;

    return rootPath + Util.join( elements, fileSeparator );
    }


  public void createTable( String schemaName, String tableName, String uri )
    {
    }

  protected abstract String getFileSeparator();

  public abstract boolean pathExists( String path );

  public abstract boolean deletePath( String path );

  public abstract boolean createPath( String path );

  protected abstract InputStream getInputStream( String path );

  protected abstract OutputStream getOutputStream( String path );

  private void loadSchemas( SchemaCatalog catalog )
    {
    String schemaProperty = getStringProperty( SCHEMAS_PROP );
    String[] schemaIdentifiers = schemaProperty.split( "," );

    for( String schemaIdentifier : schemaIdentifiers )
      catalog.createSchemaDefAndTableDefsFor( schemaIdentifier );
    }

  private void loadTables( SchemaCatalog catalog )
    {
    String tableProperty = getStringProperty( TABLES_PROP );
    String[] tableIdentifiers = tableProperty.split( "," );

    for( String tableIdentifier : tableIdentifiers )
      catalog.createTableDefFor( tableIdentifier );
    }

  private String getStringProperty( String propertyName )
    {
    return properties.getProperty( propertyName );
    }

  protected abstract Class<? extends SchemaCatalog> getCatalogClass();

  public String[] getChildIdentifiers( String identifier ) throws IOException
    {
    return getChildIdentifiers( getFileTypeFor( identifier ) );
    }

  public abstract FileType getFileTypeFor( String identifier );

  public abstract String[] getChildIdentifiers( FileType<Config> fileType ) throws IOException;

  public PlatformInfo getPlatformInfo()
    {
    return getFlowConnector().getPlatformInfo();
    }

  public abstract FlowConnector getFlowConnector();

  public LingualFlowFactory getFlowFactory( Branch branch )
    {
    return new LingualFlowFactory( this, createName(), branch );
    }

  private String createName()
    {
    return "" + System.currentTimeMillis() + "-" + Util.createUniqueID().substring( 0, 10 );
    }

  public SchemaCatalog newInstance()
    {
    try
      {
      SchemaCatalog schemaCatalog = getCatalogClass().getConstructor().newInstance();

      schemaCatalog.setPlatformBroker( this );

      schemaCatalog.initializeNew(); // initialize defaults for a new catalog and root schema

      return schemaCatalog;
      }
    catch( Exception exception )
      {
      throw new RuntimeException( "unable to construct class: " + getCatalogClass().getName(), exception );
      }
    }
  }
