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

package cascading.lingual.platform.hadoop;

import java.util.Collections;
import java.util.List;

import cascading.bind.catalog.handler.FormatHandler;
import cascading.bind.catalog.handler.ProtocolHandler;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.SchemaCatalog;

/**
 *
 */
public class HadoopCatalog extends SchemaCatalog
  {
  public static final Protocol DEFAULT_PROTOCOL = HadoopDefaultProtocolHandler.HDFS;
  public static final Format DEFAULT_FORMAT = HadoopDefaultFormatHandler.CSV;

  public HadoopCatalog()
    {
    super( DEFAULT_PROTOCOL, DEFAULT_FORMAT );
    }

  @Override
  protected List<ProtocolHandler<Protocol, Format>> createProtocolHandlers()
    {
    return Collections.<ProtocolHandler<Protocol, Format>>singletonList( new HadoopDefaultProtocolHandler() );
    }

  @Override
  protected List<FormatHandler<Protocol, Format>> createFormatHandlers()
    {
    return Collections.<FormatHandler<Protocol, Format>>singletonList( new HadoopDefaultFormatHandler() );
    }
  }
