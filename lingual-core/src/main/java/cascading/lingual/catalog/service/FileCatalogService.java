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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.json.JSONFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class FileCatalogManager can read and write the a meta-data catalog as JSON to an Input/Output stream provided
 * by the underlying platform.
 */
public class FileCatalogService extends CatalogService
  {
  private static final Logger LOG = LoggerFactory.getLogger( FileCatalogService.class );

  public FileCatalogService()
    {
    }

  public SchemaCatalog createSchemaCatalog( Protocol defaultProtocol, Format defaultFormat )
    {
    return new JSONSchemaCatalog( defaultProtocol, defaultFormat );
    }

  @Override
  public void commitCatalog( SchemaCatalog catalog )
    {
    String catalogPath = platformBroker.getFullCatalogPath();
    OutputStream outputStream = platformBroker.getOutputStream( catalogPath );

    writeAsJsonAndClose( catalogPath, outputStream, catalog );
    }

  private void writeAsJsonAndClose( String catalogPath, OutputStream outputStream, SchemaCatalog catalog )
    {
    ObjectMapper mapper = getObjectMapper();

    try
      {
      mapper.writer().withDefaultPrettyPrinter().writeValue( outputStream, catalog );

      outputStream.close();
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to write path: " + catalogPath, exception );
      }
    }

  @Override
  public SchemaCatalog openSchemaCatalog()
    {
    String catalogPath = platformBroker.getFullCatalogPath();

    LOG.info( "reading catalog from: {}", catalogPath );

    InputStream inputStream = platformBroker.getInputStream( catalogPath );

    if( inputStream == null )
      {
      LOG.info( "catalog not found at: {}", catalogPath );
      return null;
      }

    return readAsJsonAndClose( catalogPath, inputStream );
    }

  private SchemaCatalog readAsJsonAndClose( String catalogPath, InputStream inputStream )
    {
    ObjectMapper mapper = getObjectMapper();

    try
      {
      JSONSchemaCatalog schemaCatalog = mapper.readValue( inputStream, JSONSchemaCatalog.class );

      inputStream.close();

      return schemaCatalog;
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to read path: " + catalogPath, exception );
      }
    }

  private ObjectMapper getObjectMapper()
    {
    return JSONFactory.getObjectMapper();
    }
  }
