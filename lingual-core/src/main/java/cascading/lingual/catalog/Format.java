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

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import cascading.lingual.util.Util;
import com.google.common.base.Function;
import com.google.common.cache.LoadingCache;

public class Format implements Serializable
  {
  private static final Function<String, Format> factory = new Function<String, Format>()
  {
  @Override
  public Format apply( String input )
    {
    return new Format( input.toLowerCase() );
    }
  };

  private static final LoadingCache<String, Format> cache = Util.makeInternedCache( factory );

  public static final Format CSV = addFormat( "csv", ".csv" );
  public static final Format TSV = addFormat( "tsv", ".tsv" );
  public static final Format TCSV = addFormat( "tcsv", ".tcsv" );
  public static final Format TTSV = addFormat( "ttsv", ".ttsv" );

  public static Format addFormat( String name, String... extensions )
    {
    Format format = cache.getUnchecked( name );

    Collections.addAll( format.getExtensions(), extensions );

    return format;
    }

  public static Format getFormat( String name )
    {
    return cache.getUnchecked( name );
    }

  public static Format findFormatFor( String path )
    {
    if( path == null || path.isEmpty() )
      return null;

    String extension = path.replaceAll( ".*\\.([^.]+)$", "$1" );

    for( Format format : cache.asMap().values() )
      {
      if( format.extensions.contains( extension.toLowerCase() ) )
        return format;
      }

    return null;
    }

  private final String name;
  private final Set<String> extensions = new HashSet<String>();

  private Format( String name, String... extensions )
    {
    this.name = name;
    Collections.addAll( this.extensions, extensions );
    }

  public String getName()
    {
    return name;
    }

  public Set<String> getExtensions()
    {
    return extensions;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( "Format" );
    sb.append( "{name='" ).append( name ).append( '\'' );
    sb.append( '}' );
    return sb.toString();
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Format format = (Format) object;

    if( !name.equals( format.name ) )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    return name.hashCode();
    }
  }
