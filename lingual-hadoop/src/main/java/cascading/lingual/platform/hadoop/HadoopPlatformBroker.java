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

package cascading.lingual.platform.hadoop;

import java.io.IOException;
import java.net.URL;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.lingual.catalog.LingualCatalog;
import cascading.lingual.platform.PlatformBroker;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import cascading.tap.type.FileType;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
public class HadoopPlatformBroker extends PlatformBroker<JobConf>
  {
  private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger( HadoopPlatformBroker.class );

  public HadoopPlatformBroker()
    {
    }

  @Override
  public String getName()
    {
    return "hadoop";
    }

  @Override
  public JobConf getConfig()
    {
    JobConf jobConf = HadoopUtil.createJobConf( getProperties(), new JobConf() );

    if( jobConf.getJar() != null )
      return jobConf;

    String appJar = findAppJar();

    if( appJar != null )
      jobConf.setJar( appJar );

    return jobConf;
    }

  private String findAppJar()
    {
    URL url = Thread.currentThread().getContextClassLoader().getResource( "/META-INF/hadoop.job.properties" );

    if( url == null || !url.toString().startsWith( "jar" ) )
      return null;

    String path = url.toString();
    String jarPath = path.substring( 0, path.lastIndexOf( "!" ) + 1 );

    LOG.info( "using hadoop job jar: {}", jarPath );

    return jarPath;
    }

  @Override
  public FlowConnector getFlowConnector()
    {
    return new HadoopFlowConnector( getProperties() );
    }

  @Override
  public FlowProcess<JobConf> getFlowProcess()
    {
    return new HadoopFlowProcess( getConfig() );
    }

  @Override
  public LingualCatalog createCatalog()
    {
    return new HadoopCatalog( this );
    }

  @Override
  public FileType getFileTypeFor( String identifier )
    {
    return new Hfs( new TextLine(), identifier, SinkMode.KEEP );
    }

  @Override
  public String[] getChildIdentifiers( FileType<JobConf> fileType ) throws IOException
    {
    return fileType.getChildIdentifiers( getConfig() );
    }
  }
