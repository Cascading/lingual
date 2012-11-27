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

package cascading.lingual.platform.local;

import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.platform.LingualSchemeFactory;
import cascading.lingual.tap.local.TypedTextDelimited;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tuple.Fields;
import com.google.common.base.Function;
import com.google.common.collect.Table;

/**
 *
 */
public class LocalDefaultSchemeFactory extends LingualSchemeFactory
  {
  public LocalDefaultSchemeFactory()
    {
    }

  @Override
  protected void initialize( Table<Protocol, Format, Function<Fields, Scheme>> table )
    {
    table.put( Protocol.FILE, Format.CSV, new Function<Fields, Scheme>()
    {
    @Override
    public Scheme apply( Fields fields )
      {
      return new TextDelimited( fields, true, ",", "\"" );
      }
    } );

    table.put( Protocol.FILE, Format.TSV, new Function<Fields, Scheme>()
    {
    @Override
    public Scheme apply( Fields fields )
      {
      return new TextDelimited( fields, true, "\t", "\"" );
      }
    } );

    table.put( Protocol.FILE, Format.TCSV, new Function<Fields, Scheme>()
    {
    @Override
    public Scheme apply( Fields fields )
      {
      return new TypedTextDelimited( fields, ",", "\"" );
      }
    } );

    table.put( Protocol.FILE, Format.TTSV, new Function<Fields, Scheme>()
    {
    @Override
    public Scheme apply( Fields fields )
      {
      return new TypedTextDelimited( fields, "\t", "\"" );
      }
    } );
    }
  }
