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

import java.util.Properties;

import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * ProviderFactory is the optional interface provider implementations must implement.
 * <p/>
 * If not directly implemented, at least one create method must be implemented.
 */
public interface ProviderFactory
  {
  /**
   * Optional description of this provider.
   *
   * @return a String
   */
  String getDescription();

  Tap createTap( String protocol, Scheme scheme, String identifier, SinkMode mode, Properties properties );

  Tap createTap( Scheme scheme, String identifier, SinkMode mode, Properties properties );

  Tap createTap( Scheme scheme, String identifier, Properties properties );

  Tap createTap( Scheme scheme, String identifier, SinkMode mode );

  Scheme createScheme( String protocol, String format, Fields fields, Properties properties );

  Scheme createScheme( String format, Fields fields, Properties properties );

  Scheme createScheme( Fields fields, Properties properties );

  Scheme createScheme( Fields fields );
  }
