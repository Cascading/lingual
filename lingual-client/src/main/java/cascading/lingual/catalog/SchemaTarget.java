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

package cascading.lingual.catalog;

import java.util.Collection;

import cascading.lingual.common.Printer;
import cascading.lingual.platform.PlatformBroker;

/**
 *
 */
public class SchemaTarget extends Target
  {
  public SchemaTarget( Printer printer, CatalogOptions options )
    {
    super( printer, options );
    }

  @Override
  protected boolean performRename( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    return catalog.renameSchemaDef( getOptions().getSchemaName(), getOptions().getRenameName() );
    }

  @Override
  protected boolean performRemove( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    return catalog.removeSchemaDef( getOptions().getSchemaName() );
    }

  @Override
  protected String performAdd( PlatformBroker platformBroker )
    {
    return platformBroker.getCatalog().createSchemaDefAndTableDefsFor( getOptions().getAddURI() );
    }

  @Override
  protected Collection<String> performGetNames( PlatformBroker platformBroker )
    {
    return platformBroker.getCatalog().getSchemaNames();
    }
  }
