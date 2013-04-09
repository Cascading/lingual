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

package cascading.lingual.platform.local;

import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.platform.LingualFormatHandler;
import cascading.lingual.tap.local.SQLTypedTextDelimited;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tuple.Fields;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.google.common.base.Function;
import com.google.common.collect.Table;

import static cascading.lingual.catalog.FormatProperties.EXTENSIONS;
import static cascading.lingual.platform.local.LocalDefaultProtocolHandler.FILE;

/**
 *
 */
@JsonAutoDetect(
  fieldVisibility = JsonAutoDetect.Visibility.NONE,
  getterVisibility = JsonAutoDetect.Visibility.NONE,
  setterVisibility = JsonAutoDetect.Visibility.NONE)
public class LocalDefaultFormatHandler extends LingualFormatHandler
  {
  public static final Format CSV = Format.getFormat( "csv" );
  public static final Format TSV = Format.getFormat( "tsv" );
  public static final Format TCSV = Format.getFormat( "tcsv" );
  public static final Format TTSV = Format.getFormat( "ttsv" );

  public LocalDefaultFormatHandler()
    {
    getDefaults().addProperty( CSV, EXTENSIONS, ".csv" );
    getDefaults().addProperty( TSV, EXTENSIONS, ".tsv" );
    getDefaults().addProperty( TCSV, EXTENSIONS, ".tcsv" );
    getDefaults().addProperty( TTSV, EXTENSIONS, ".ttsv" );
    }

  @Override
  protected void initialize( Table<Protocol, Format, Function<Fields, Scheme>> table )
    {
    table.put( FILE, CSV, new Function<Fields, Scheme>()
    {
    @Override
    public Scheme apply( Fields fields )
      {
      return new TextDelimited( fields, true, ",", "\"" );
      }
    } );

    table.put( FILE, TSV, new Function<Fields, Scheme>()
    {
    @Override
    public Scheme apply( Fields fields )
      {
      return new TextDelimited( fields, true, "\t", "\"" );
      }
    } );

    table.put( FILE, TCSV, new Function<Fields, Scheme>()
    {
    @Override
    public Scheme apply( Fields fields )
      {
      return new SQLTypedTextDelimited( fields, ",", "\"" );
      }
    } );

    table.put( FILE, TTSV, new Function<Fields, Scheme>()
    {
    @Override
    public Scheme apply( Fields fields )
      {
      return new SQLTypedTextDelimited( fields, "\t", "\"" );
      }
    } );
    }
  }
