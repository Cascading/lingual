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

package cascading.lingual.platform.local;

import java.io.IOException;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.lingual.catalog.LingualCatalog;
import cascading.lingual.platform.PlatformBroker;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.local.FileTap;
import cascading.tap.type.FileType;

/**
 *
 */
public class LocalPlatformBroker extends PlatformBroker<Properties>
  {
  public LocalPlatformBroker()
    {
    }

  @Override
  public String getName()
    {
    return "local";
    }

  @Override
  public LingualCatalog createCatalog()
    {
    return new LocalCatalog( this );
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
  public String[] getChildIdentifiers( FileType<Properties> fileType ) throws IOException
    {
    return fileType.getChildIdentifiers( getConfig() );
    }

  @Override
  public Properties getConfig()
    {
    return getProperties();
    }
  }
