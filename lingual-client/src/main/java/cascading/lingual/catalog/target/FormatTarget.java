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

package cascading.lingual.catalog.target;

import java.util.Collection;
import java.util.List;

import cascading.lingual.catalog.CatalogOptions;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.common.Printer;
import cascading.lingual.platform.PlatformBroker;

/**
 *
 */
public class FormatTarget extends CRUDTarget
  {
  public FormatTarget( Printer printer, CatalogOptions options )
    {
    super( printer, options );
    }

  @Override
  protected boolean performRename( PlatformBroker platformBroker )
    {
    return false;
    }

  @Override
  protected boolean performRemove( PlatformBroker platformBroker )
    {
    return false;
    }

  @Override
  protected String performAdd( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    Format format = Format.getFormat( getOptions().getFormatName() );

    if( format == null )
      throw new IllegalArgumentException( "add action must have a format name value" );

    String schemaName = getOptions().getSchemaName();
    List<String> extensions = getOptions().getExtensions();

    catalog.addFormat( schemaName, format, extensions );

    return format.getName();
    }

  @Override
  protected Collection<String> performGetNames( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    String schemaName = getOptions().getSchemaName();
    if( schemaName != null && !schemaName.isEmpty() )
      return catalog.getFormatNames( getOptions().getSchemaName() );
    else
      return catalog.getFormatNames();

    }
  }
