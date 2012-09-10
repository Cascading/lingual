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

import java.util.Properties;

import cascading.lingual.catalog.Platform;
import cascading.lingual.platform.hadoop.HadoopPlatformBroker;
import cascading.lingual.platform.local.LocalPlatformBroker;

/**
 *
 */
public class PlatformBrokerFactory
  {
  public static PlatformBroker createPlatformBroker( String platformName, Properties properties )
    {
    return getPlatformBroker( resolvePlatformName( platformName ), properties );
    }

  private static PlatformBroker getPlatformBroker( Platform platform, Properties properties )
    {
    switch( platform )
      {
      case LOCAL:
        return new LocalPlatformBroker( properties );

      case HADOOP:
        return new HadoopPlatformBroker( properties );

      default:
        throw new IllegalArgumentException( "unknown platform: " + platform );
      }
    }

  private static Platform resolvePlatformName( String platformName )
    {
    return Platform.valueOf( platformName.toUpperCase() );
    }
  }
