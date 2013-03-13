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

package cascading.lingual.catalog;

import java.util.Map;

import cascading.management.CascadingServices;
import cascading.management.DocumentService;
import cascading.provider.CascadingService;

/**
 *
 */
public abstract class CatalogManager implements CascadingService
  {
  public static final String CATALOG_SERVICE_CLASS_PROPERTY = "cascading.management.catalog.service.classname";

  private Map<Object, Object> properties;

  DocumentService documentService = new CascadingServices.NullDocumentService();

  @Override
  public void setProperties( Map<Object, Object> properties )
    {
    this.properties = properties;
    }

  @Override
  public void startService()
    {
    }

  @Override
  public void stopService()
    {
    }

  @Override
  public boolean isEnabled()
    {
    return true;
    }

  public abstract void writeCatalog( SchemaCatalog catalog );

  public abstract SchemaCatalog readCatalog();
  }
