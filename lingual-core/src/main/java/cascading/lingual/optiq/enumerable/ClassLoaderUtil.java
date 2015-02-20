/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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
package cascading.lingual.optiq.enumerable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import cascading.lingual.catalog.ProviderDef;
import cascading.lingual.catalog.TableDef;
import cascading.lingual.platform.LingualFlowFactory;
import cascading.lingual.platform.PlatformBroker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility Class to manage ClassLoaders within lingual.
 */
public class ClassLoaderUtil
  {
  /** logger. */
  private static final Logger LOG = LoggerFactory.getLogger( ClassLoaderUtil.class );

  public static ClassLoader getJarClassLoader( PlatformBroker platformBroker, LingualFlowFactory flowFactory )
    {
    if( flowFactory.getJars().isEmpty() ) // will retrieve remote jars and make them local
      return null;

    String[] jarsArray = flowFactory.getJarsArray();

    LOG.debug( "creating context loader for: {}", Arrays.toString( jarsArray ) );

    return platformBroker.getUrlClassLoader( jarsArray );
    }


  /**
   * Returns the paths to the jar files, which are necessary to interact with the given TableDef.
   * @param platformBroker The platformBroker of the current platform.
   * @param tableDef  The table definition to find the jars for.
   * @return an array of jar paths.
   */
  public static String[] getJarPaths( PlatformBroker platformBroker, TableDef tableDef )
    {
    Set<String> jars = new HashSet<String>();
    String rootPath = platformBroker.getFullProviderPath();

    ProviderDef protocolProvider = tableDef.getProtocolProvider();

    if( protocolProvider != null && protocolProvider.getIdentifier() != null )
      jars.add( platformBroker.makePath( rootPath, protocolProvider.getIdentifier() ) );

    ProviderDef formatProvider = tableDef.getFormatProvider();

    if( formatProvider != null && formatProvider.getIdentifier() != null )
      jars.add( platformBroker.makePath( rootPath, formatProvider.getIdentifier() ) );

    return jars.toArray( new String[ jars.size() ] );
    }
  }
