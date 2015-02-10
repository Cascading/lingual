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

package cascading.lingual.catalog;

import java.io.Serializable;
import java.util.Properties;

import cascading.lingual.catalog.provider.ProviderFactory;
import cascading.lingual.platform.provider.DefaultFactory;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import static cascading.lingual.util.Reflection.loadClassSafe;
import static cascading.lingual.util.Reflection.newInstanceSafe;

/** is sub-classed by the dynamic class in FactoryProviderJarCLITest */
public class TestProviderFactory implements ProviderFactory, Serializable
  {
  transient DefaultFactory factory;

  public TestProviderFactory()
    {
    String [] classNames = { "cascading.lingual.platform.local.LocalDefaultFactory",
                             "cascading.lingual.platform.hadoop.HadoopDefaultFactory",
                             "cascading.lingual.platform.hadoop2.Hadoop2MR1DefaultFactory",
                             "cascading.lingual.platform.tez.TezDefaultFactory" };

    for( String className: classNames)
      {
      factory = (DefaultFactory) newInstanceSafe( loadClassSafe( className ) );
      if( factory != null )
        break;
      }

    if( factory == null )
      throw new IllegalStateException( "unable to load factory from known classes" );

    }

  @Override
  public String getDescription()
    {
    return null;
    }

  @Override
  public Tap createTap( String protocol, Scheme scheme, String identifier, SinkMode mode, Properties properties )
    {
    return factory.createTap( protocol, scheme, identifier, mode, properties );
    }

  @Override
  public Tap createTap( Scheme scheme, String identifier, SinkMode mode, Properties properties )
    {
    return null;
    }

  @Override
  public Tap createTap( Scheme scheme, String identifier, Properties properties )
    {
    return null;
    }

  @Override
  public Tap createTap( Scheme scheme, String identifier, SinkMode mode )
    {
    return null;
    }

  @Override
  public Scheme createScheme( String protocol, String format, Fields fields, Properties properties )
    {
    return factory.createScheme( protocol, format, fields, properties );
    }

  @Override
  public Scheme createScheme( String format, Fields fields, Properties properties )
    {
    return null;
    }

  @Override
  public Scheme createScheme( Fields fields, Properties properties )
    {
    return null;
    }

  @Override
  public Scheme createScheme( Fields fields )
    {
    return null;
    }
  }
