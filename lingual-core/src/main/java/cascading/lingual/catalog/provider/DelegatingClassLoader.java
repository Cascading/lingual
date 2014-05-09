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

package cascading.lingual.catalog.provider;

import java.security.SecureClassLoader;

/**
 * ClassLoader that delegates to a collection of child ClassLoaders.
 */
class DelegatingClassLoader extends SecureClassLoader
  {
  /** ClassLoaders to use*/
  private ClassLoader[] delegates;

  /**
   * Constructs a new DelegatingClassLoader with the given delegates.
   * @param delegates ClassLoader instances to try.
   */
  public DelegatingClassLoader( ClassLoader... delegates )
    {
    this.delegates = delegates;
    }


  @Override
  public Class<?> loadClass( String name ) throws ClassNotFoundException
    {
    for( ClassLoader loader : delegates )
      {
      try
        {
        return loader.loadClass( name );
        }
      catch( ClassNotFoundException exception )
        {
        // ignore
        }
      }
    throw new ClassNotFoundException( name );
    }
  }
