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

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import cascading.bind.catalog.Stereotype;
import cascading.lingual.catalog.CatalogOptions;
import cascading.lingual.catalog.ProviderDef;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.SchemaDef;
import cascading.lingual.catalog.builder.StereotypeBuilder;
import cascading.lingual.common.Printer;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.type.SQLTypeMap;
import cascading.lingual.type.TypeMap;
import cascading.tuple.Fields;

import static java.util.Arrays.asList;

/**
 *
 */
public class StereotypeTarget extends CRUDTarget
  {
  TypeMap typeMap = new SQLTypeMap();

  public StereotypeTarget( Printer printer, CatalogOptions options )
    {
    super( printer, options );
    }

  @Override
  protected boolean performRename( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    String schemaName = getOptions().getSchemaName();
    String stereotypeName = getOptions().getStereotypeName();
    String renameName = getOptions().getRenameName();

    return catalog.renameStereotype( schemaName, stereotypeName, renameName );
    }

  @Override
  protected boolean performRemove( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    String schemaName = getOptions().getSchemaName();
    String stereotypeName = getOptions().getStereotypeName();

    return catalog.removeStereotype( schemaName, stereotypeName );
    }

  @Override
  protected Object getSource( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    SchemaDef schemaDef = catalog.getSchemaDef( getOptions().getSchemaName() );

    if( schemaDef == null )
      return null;

    return catalog.getSchemaDef( getOptions().getSchemaName() ).getStereotype( getOptions().getStereotypeName() );
    }

  @Override
  protected String getRequestedSourceName()
    {
    return getOptions().getStereotypeName();
    }

  @Override
  protected void validateAdd( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    String schemaName = getOptions().getSchemaName();
    String providerName = getOptions().getProviderName();

    if( providerName != null )
      {
      ProviderDef providerDef = catalog.findProviderFor( schemaName, providerName );

      if( providerDef == null )
        throw new IllegalArgumentException( "provider not registered to schema: " + providerName );
      }
    }

  @Override
  protected List<String> performAdd( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    String schemaName = getOptions().getSchemaName();
    String stereotypeName = getOptions().getStereotypeName();
    List<String> columns = getOptions().getColumns();
    List<String> types = getOptions().getTypes();
    Fields fields = createFields( columns, types );

    catalog.createStereotype( schemaName, stereotypeName, fields );

    return asList( stereotypeName );
    }

  @Override
  protected Collection<String> performGetNames( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();

    String schemaName = getOptions().getSchemaName();

    if( schemaName != null && !schemaName.isEmpty() )
      return catalog.getStereotypeNames( schemaName );
    else
      return catalog.getStereotypeNames();
    }

  @Override
  protected Map performShow( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    String schemaName = getOptions().getSchemaName();
    String stereotypeName = getOptions().getStereotypeName();
    Stereotype stereotype = catalog.getSchemaDefChecked( schemaName ).getStereotypeChecked( stereotypeName );

    return new StereotypeBuilder().format( stereotype );
    }

  private Fields createFields( List<String> columns, List<String> types )
    {
    Fields fields = new Fields( columns.toArray( new Comparable[ columns.size() ] ) );

    Type[] typeArray = typeMap.getTypesFor( types.toArray( new String[ types.size() ] ) );

    return fields.applyTypes( typeArray );
    }
  }
