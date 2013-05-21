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

package cascading.lingual.platform.local;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.platform.PlatformBroker;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.local.FileTap;
import cascading.tap.type.FileType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class LocalPlatformBroker extends PlatformBroker<Properties>
  {
  private static final Logger LOG = LoggerFactory.getLogger( LocalPlatformBroker.class );

  public LocalPlatformBroker()
    {
    }

  @Override
  public String getName()
    {
    return "local";
    }

  @Override
  public Class<? extends SchemaCatalog> getCatalogClass()
    {
    return LocalCatalog.class;
    }

  @Override
  public FlowConnector getFlowConnector()
    {
    return new LocalFlowConnector( getProperties() );
    }

  @Override
  public FlowProcess<Properties> getFlowProcess()
    {
    return new LocalFlowProcess( getConfig() );
    }

  @Override
  public FileType getFileTypeFor( String identifier )
    {
    return new FileTap( new TextLine(), identifier, SinkMode.KEEP );
    }

  @Override
  public Properties getConfig()
    {
    return getProperties();
    }

  @Override
  public String getFullPath( String identifier )
    {
    try
      {
      File canonicalFile = new File( identifier ).getCanonicalFile();

      return findActualPath( canonicalFile.getParent(), canonicalFile.toString() );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to get full path for: " + identifier, exception );
      }
    }

  @Override
  public boolean pathExists( String path )
    {
    File file = new File( path );

    return file.exists();
    }

  @Override
  public boolean createPath( String path )
    {
    return new File( path ).mkdirs();
    }

  @Override
  public boolean deletePath( String path )
    {
    File file = new File( path );

    return file.delete();
    }

  @Override
  public InputStream getInputStream( String path )
    {
    if( path == null || path.isEmpty() )
      return null;

    File file = new File( path );

    if( !file.exists() )
      return null;

    try
      {
      return new FileInputStream( file );
      }
    catch( FileNotFoundException exception )
      {
      throw new RuntimeException( "unable to find file: " + path, exception );
      }
    }

  @Override
  public OutputStream getOutputStream( String path )
    {
    if( path == null || path.isEmpty() )
      return null;

    File file = new File( path );

    if( file.exists() )
      file.delete();
    else
      file.getParentFile().mkdirs();

    try
      {
      return new FileOutputStream( file, false );
      }
    catch( FileNotFoundException exception )
      {
      throw new RuntimeException( "unable to find file: " + path, exception );
      }
    }

  @Override
  public String getTempPath()
    {
    String tempDir = System.getenv( "TEMPDIR" );

    if( tempDir == null || tempDir.isEmpty() )
      tempDir = System.getenv( "TMPDIR" );

    if( tempDir == null || tempDir.isEmpty() )
      {
      LOG.warn( "neither TEMPDIR or TMPDIR is set, using '/tmp' as the temporary data path" );
      tempDir = "/tmp/";
      }

    return getFullPath( tempDir );
    }

  @Override
  public String getFileSeparator()
    {
    return File.separator;
    }
  }
