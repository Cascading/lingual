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

package cascading.lingual.catalog.service;

import java.util.Map;

import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.platform.PlatformBroker;
import cascading.provider.CascadingService;

/**
 *
 */
public abstract class CatalogService implements CascadingService
  {
  public static final String CATALOG_SERVICE_CLASS_PROPERTY = "cascading.management.catalog.service.classname";
  PlatformBroker platformBroker;

  private Map<Object, Object> properties;

  public CatalogService()
    {
    }

  @Override
  public void setProperties( Map<Object, Object> properties )
    {
    this.properties = properties;
    }

  public void setPlatformBroker( PlatformBroker platformBroker )
    {
    this.platformBroker = platformBroker;
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

  public abstract SchemaCatalog createSchemaCatalog( Protocol defaultProtocol, Format defaultFormat );

  public abstract SchemaCatalog openSchemaCatalog();

  public abstract void commitCatalog( SchemaCatalog catalog );
  }
