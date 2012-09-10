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
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.lingual.catalog.LingualCatalog;
import cascading.lingual.jdbc.Driver;
import cascading.lingual.optiq.meta.Branch;
import cascading.tap.type.FileType;
import cascading.util.Util;

/**
 *
 */
public abstract class PlatformBroker<Config>
  {
  private final Properties properties;
  private LingualCatalog catalog;

  protected PlatformBroker( Properties properties )
    {
    this.properties = properties;
    }

  public Properties getProperties()
    {
    return properties;
    }

  public abstract Config getConfig();

  public abstract FlowProcess<Config> getFlowProcess();

  public LingualCatalog getCatalog()
    {
    if( catalog == null )
      catalog = createCatalog();

    return catalog;
    }

  protected abstract LingualCatalog createCatalog();

  public String[] getChildIdentifiers( String identifier ) throws IOException
    {
    return getChildIdentifiers( getFileTypeFor( identifier ) );
    }

  public abstract FileType getFileTypeFor( String identifier );

  public abstract String[] getChildIdentifiers( FileType<Config> fileType ) throws IOException;

  public abstract FlowConnector getFlowConnector();

  public LingualFlowFactory getFlowFactory( Branch branch )
    {
    return new LingualFlowFactory( this, createName(), branch );
    }

  private String createResultPath( String name )
    {
    String path = properties.getProperty( Driver.RESULT_PATH_PROP, System.getenv( "TEMPDIR" ) );

    if( !path.endsWith( "/" ) )
      path += "/";

    return path + name;
    }

  private String createName()
    {
    return "" + System.currentTimeMillis() + "-" + Util.createUniqueID().substring( 0, 10 );
    }
  }
