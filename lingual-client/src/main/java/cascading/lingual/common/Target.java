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

package cascading.lingual.common;

import cascading.lingual.catalog.CatalogOptions;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.platform.PlatformBroker;

/**
 *
 */
public abstract class Target
  {
  protected String name = getClass().getSimpleName().replaceAll( "Target$", "" ).toLowerCase();
  protected final Printer printer;
  protected final CatalogOptions options;

  public Target( Printer printer, CatalogOptions options )
    {
    this.printer = printer;
    this.options = options;
    }

  public String getTargetType()
    {
    return name;
    }

  public Printer getPrinter()
    {
    return printer;
    }

  public CatalogOptions getOptions()
    {
    return options;
    }

  public abstract boolean handle( PlatformBroker platformBroker );

  protected void verifySchema( SchemaCatalog catalog, String schemaName )
    {
    if( schemaName == null )
      throw new IllegalArgumentException( "schema name must be specified" );

    if( !catalog.getSchemaNames().contains( schemaName.toLowerCase() ) )
      throw new IllegalArgumentException( "schema does not exist: " + schemaName );
    }
  }
