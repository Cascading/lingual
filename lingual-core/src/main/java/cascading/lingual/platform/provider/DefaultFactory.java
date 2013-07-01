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

package cascading.lingual.platform.provider;

import java.util.Properties;

import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class DefaultFactory
  {
  private static final Logger LOG = LoggerFactory.getLogger( DefaultFactory.class );

  public Tap createTap( String protocol, Scheme scheme, String identifier, SinkMode mode, Properties properties )
    {
    if( LOG.isDebugEnabled() )
      {
      LOG.debug( "with protocol: {}", protocol );
      LOG.debug( "creating tap with scheme: {}, mode: {}", scheme.getClass().getName(), mode );
      }

    return newTap( scheme, identifier, mode );
    }

  protected abstract Tap newTap( Scheme scheme, String identifier, SinkMode mode );

  public Scheme createScheme( String protocol, String format, Fields fields, Properties properties )
    {
    String delimiter = properties.getProperty( "delimiter", "," );
    String quote = properties.getProperty( "quote", "\"" );
    boolean typed = Boolean.parseBoolean( properties.getProperty( "typed", "false" ) );

    if( LOG.isDebugEnabled() )
      {
      LOG.debug( "with protocol: {}, and format: {}", protocol, format );
      LOG.debug( "creating scheme with delimiter: '{}', quote: '{}', typed: {}", new Object[]{delimiter, quote, typed} );
      }

    return newScheme( fields, delimiter, quote, typed );
    }

  protected abstract Scheme newScheme( Fields fields, String delimiter, String quote, boolean typed );
  }
