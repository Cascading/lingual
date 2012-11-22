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

  public boolean hasBeenInitialized()
    {
    return pathExists( getFullCatalogPath() );
    }

  public void writeCatalog()
    {
    String catalogPath = getFullCatalogPath();

    OutputStream outputStream = getOutputStream( catalogPath );

    if( true )
      writeAsObject( catalogPath, outputStream );
    else
      writeAsJson( catalogPath, outputStream );
    }

  public String getFullCatalogPath()
    {
    String catalogPath = getStringProperty( CATALOG_PROP );

    return makeFullCatalogFilePath( catalogPath );
    }

  private void writeAsObject( String catalogPath, OutputStream outputStream )
    {
    try
      {
      ObjectOutputStream objectOutputStream = new ObjectOutputStream( outputStream );

      objectOutputStream.writeObject( getCatalog() );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to write path: " + catalogPath, exception );
      }
    }

  private void writeAsJson( String catalogPath, OutputStream outputStream )
    {
    ObjectMapper mapper = getObjectMapper();

    try
      {
      mapper.writeValue( outputStream, getCatalog() );
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

    catalog.initialize();

    if( properties.containsKey( SCHEMA_PROP ) )
      loadSchemas( catalog );

    if( properties.containsKey( TABLE_PROP ) )
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
      return readAsObject( catalogPath, inputStream );
    else
      return readAsJson( catalogPath, inputStream );
    }

  private SchemaCatalog readAsObject( String catalogPath, InputStream inputStream )
    {
    try
      {
      ObjectInputStream objectInputStream = new ObjectInputStream( inputStream );
      return (SchemaCatalog) objectInputStream.readObject();
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

  private SchemaCatalog readAsJson( String catalogPath, InputStream inputStream )
    {
    ObjectMapper mapper = getObjectMapper();

    try
      {
      SchemaCatalog schemaCatalog = mapper.readValue( inputStream, getCatalogClass() );

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

  private String makeFullCatalogFilePath( String catalogPath )
    {
    String metaDataPath = properties.getProperty( META_DATA_PATH_PROP, META_DATA_PATH );
    String metaDataFile = properties.getProperty( CATALOG_FILE_PROP, CATALOG_FILE );

    return makeFullCatalogFilePath( getFileSeparator(), catalogPath, metaDataPath, metaDataFile );
    }

  public static String makeFullCatalogFilePath( String fileSeparator, String catalogPath, String metaDataPath, String metaDataFile )
    {
    if( catalogPath == null )
      catalogPath = ".";

    if( !catalogPath.endsWith( fileSeparator ) )
      catalogPath += fileSeparator;

    return catalogPath + metaDataPath + fileSeparator + metaDataFile;
    }

  protected abstract String getFileSeparator();

  public abstract boolean pathExists( String path );

  public abstract boolean deletePath( String path );

  protected abstract InputStream getInputStream( String path );

  protected abstract OutputStream getOutputStream( String path );

  private void loadSchemas( SchemaCatalog catalog )
    {
    String schemaProperty = getStringProperty( SCHEMA_PROP );
    String[] schemaIdentifiers = schemaProperty.split( "," );

    for( String schemaIdentifier : schemaIdentifiers )
      catalog.createSchemaFor( schemaIdentifier );
    }

  private void loadTables( SchemaCatalog catalog )
    {
    String tableProperty = getStringProperty( TABLE_PROP );
    String[] tableIdentifiers = tableProperty.split( "," );

    for( String tableIdentifier : tableIdentifiers )
      catalog.createTableFor( tableIdentifier );
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

      return schemaCatalog;
      }
    catch( Exception exception )
      {
      throw new RuntimeException( "unable to construct class: " + getCatalogClass().getName(), exception );
      }
    }
  }
