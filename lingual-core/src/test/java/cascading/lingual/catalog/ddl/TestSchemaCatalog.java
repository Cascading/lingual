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

package cascading.lingual.catalog.ddl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import cascading.bind.catalog.handler.FormatHandler;
import cascading.bind.catalog.handler.ProtocolHandler;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.SchemaDef;

/**
 *
 */
public class TestSchemaCatalog extends SchemaCatalog
  {
  public TestSchemaCatalog()
    {
    }

  protected TestSchemaCatalog( Protocol defaultProtocol, Format defaultFormat )
    {
    super( defaultProtocol, defaultFormat );

    HashMap<String, List<String>> protocolProperties = new HashMap<String, List<String>>();

    protocolProperties.put( "fakeProperty", new ArrayList<String>() );

    getRootSchemaDef().addProtocolProperties( defaultProtocol, protocolProperties );

    HashMap<String, List<String>> formatProperties = new HashMap<String, List<String>>();

    formatProperties.put( "fakeProperty", new ArrayList<String>() );

    getRootSchemaDef().addFormatProperties( defaultFormat, formatProperties );
    }

  @Override
  protected List<ProtocolHandler<Protocol, Format>> createProtocolHandlers( SchemaDef schemaDef )
    {
    return Collections.emptyList();
    }

  @Override
  protected List<FormatHandler<Protocol, Format>> createFormatHandlers( SchemaDef schemaDef )
    {
    return Collections.emptyList();
    }
  }
