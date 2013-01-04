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

package cascading.lingual.catalog.json;

import cascading.lingual.platform.PlatformBroker;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.module.SimpleSerializers;

/**
 *
 */
public class JSONFactory
  {
  public static ObjectMapper getObjectMapper( PlatformBroker platformBroker )
    {
    ObjectMapper mapper = new ObjectMapper();

    SimpleModule module = new SimpleModule( "LingualModule" );

    addSerializers( platformBroker, module );
    addDeserializers( platformBroker, module );

    mapper.registerModule( module );

    return mapper;
    }

  private static void addDeserializers( PlatformBroker platformBroker, SimpleModule module )
    {
    SimpleDeserializers deserializers = new SimpleDeserializers();


    module.setDeserializers( deserializers );
    }

  private static void addSerializers( PlatformBroker platformBroker, SimpleModule module )
    {
    SimpleSerializers serializers = new SimpleSerializers();


    module.setSerializers( serializers );
    }

  }
