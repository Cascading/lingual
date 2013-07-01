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

package cascading.lingual.platform;

import java.util.Collection;

import cascading.bind.catalog.Stereotype;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.ProviderDef;
import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import com.google.common.base.Function;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

/**
 *
 */
public abstract class TableLingualFormatHandler extends LingualFormatHandler
  {
  private transient Table<Protocol, Format, Function<Fields, Scheme>> table;

  protected TableLingualFormatHandler( ProviderDef providerDef )
    {
    super( providerDef );
    }

  protected Table<Protocol, Format, Function<Fields, Scheme>> getTable()
    {
    if( table == null )
      {
      table = HashBasedTable.create();
      initialize( table );
      }

    return table;
    }

  protected abstract void initialize( Table<Protocol, Format, Function<Fields, Scheme>> table );

  @Override
  public boolean handles( Protocol protocol, Format format )
    {
    return getTable().containsRow( protocol ) && getTable().containsColumn( format );
    }

  @Override
  public Collection<? extends Format> getFormats()
    {
    return getTable().columnKeySet();
    }

  @Override
  public Scheme createScheme( Stereotype<Protocol, Format> stereotype, Protocol protocol, Format format )
    {
    Function<Fields, Scheme> schemeFunction = getTable().get( protocol, format );

    if( schemeFunction == null )
      throw new IllegalStateException( "no scheme found for protocol: " + protocol + ", and format: " + format );

    return schemeFunction.apply( stereotype.getFields() );
    }
  }
