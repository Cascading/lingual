/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLStreamHandlerFactory;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.SchemaCatalogManager;
import cascading.lingual.catalog.SchemaDef;
import cascading.pipe.Pipe;
import cascading.tap.type.FileType;

/**
 * Implementation of the PlatformBroker to be used by {@link cascading.lingual.flow.SQLPlanner}.
 */
public class PlannerPlatformBroker extends PlatformBroker<Properties>
  {
  private Pipe tail;

  public PlannerPlatformBroker()
    {
    setProperties( new Properties() );
    }

  public Pipe getTail()
    {
    return tail;
    }

  public void setTail( Pipe tail )
    {
    this.tail = tail;
    }

  @Override
  public String getName()
    {
    return "planner";
    }

  @Override
  public Properties getDefaultConfig()
    {
    return new Properties();
    }

  @Override
  public Properties getSystemConfig()
    {
    return new Properties();
    }

  @Override
  public Properties getPlannerConfig()
    {
    return null;
    }

  @Override
  public Format getDefaultFormat()
    {
    return null;
    }

  @Override
  public Protocol getDefaultProtocol()
    {
    return null;
    }

  @Override
  public FlowProcess<Properties> getFlowProcess()
    {
    return null;
    }

  @Override
  public String getFileSeparator()
    {
    return File.separator;
    }

  @Override
  public String getTempPath()
    {
    return null;
    }

  @Override
  public String getFullPath( String identifier )
    {
    return null;
    }

  @Override
  public boolean pathExists( String path )
    {
    return false;
    }

  @Override
  public boolean deletePath( String path )
    {
    return false;
    }

  @Override
  public boolean createPath( String path )
    {
    return false;
    }

  @Override
  public InputStream getInputStream( String path )
    {
    return null;
    }

  @Override
  public OutputStream getOutputStream( String path )
    {
    return null;
    }

  @Override
  public FileType getFileTypeFor( String identifier )
    {
    return null;
    }

  @Override
  public FlowConnector getFlowConnector()
    {
    return null;
    }

  @Override
  protected URI toURI( String qualifiedPath )
    {
    return null;
    }

  @Override
  protected URLStreamHandlerFactory getURLStreamHandlerFactory()
    {
    return null;
    }

  @Override
  public synchronized SchemaCatalogManager getCatalogManager()
    {
    return null;
    }

  @Override
  public SchemaDef getResultSchemaDef()
    {
    return null;
    }

  @Override
  public boolean hasResultSchemaDef()
    {
    return false;
    }
  }
