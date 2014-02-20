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

package cascading.lingual.catalog.json;

import cascading.bind.json.FieldsDeserializer;
import cascading.bind.json.FieldsSerializer;
import cascading.tuple.Fields;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.datatype.guava.GuavaModule;

/**
 *
 */
public class JSONFactory
  {
  public static ObjectMapper getObjectMapper()
    {
    ObjectMapper mapper = new ObjectMapper();

    mapper.registerModule( new GuavaModule() );

    SimpleModule module = new SimpleModule( "LingualModule" );

    addSerializers( module );
    addDeserializers( module );

    mapper.registerModule( module );

    return mapper;
    }

  private static void addDeserializers( SimpleModule module )
    {
    SimpleDeserializers deserializers = new SimpleDeserializers();

    deserializers.addDeserializer( Fields.class, new FieldsDeserializer() );

    module.setDeserializers( deserializers );
    }

  private static void addSerializers( SimpleModule module )
    {
    SimpleSerializers serializers = new SimpleSerializers();

    serializers.addSerializer( new FieldsSerializer() );

    module.setSerializers( serializers );
    }

  }
