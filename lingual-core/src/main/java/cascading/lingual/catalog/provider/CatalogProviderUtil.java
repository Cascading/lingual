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

package cascading.lingual.catalog.provider;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

public class CatalogProviderUtil
  {
  /**
   * Load *.properties resource into java.util.Properties
   *
   * @param jarFile the provider jar file.
   * @return properties determining provider setup
   * @throws InvalidProviderException
   */
  public static Properties getProviderProperties( File jarFile )
    {
    try
      {
      JarInputStream jarInputStream = new JarInputStream( new BufferedInputStream( new FileInputStream( jarFile ) ) );
      // position the input stream at the given resource.
      JarEntry entry = jarInputStream.getNextJarEntry();

      while( ( entry != null ) && ( !ProviderDefinition.CASCADING_BIND_PROVIDER_PROPERTIES.equals( entry.getName() ) ) )
        entry = jarInputStream.getNextJarEntry();

      if( entry == null )
        throw new InvalidProviderException( "unable to find resource " + ProviderDefinition.CASCADING_BIND_PROVIDER_PROPERTIES + " in provider jar" );

      Properties properties = new Properties();

      // calling load on JarInputStream gets only the current resource.
      properties.load( jarInputStream );

      return properties;
      }
    catch( IOException ioe )
      {
      throw new InvalidProviderException( "unable to read resource " + ProviderDefinition.CASCADING_BIND_PROVIDER_PROPERTIES + " in provider jar", ioe );
      }
    }
  }

