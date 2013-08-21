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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.cache.LoadingCache;

import static cascading.lingual.util.MiscCollection.makeInternedCache;

/**
 * Class Format is an enhanced "enum" type that allows for runtime membership used for defining new "formats"
 * used during table and identifier runtime resolution.
 */
public class Format implements Serializable, Comparable<Format>
  {
  private static final Function<String, Format> factory = new Function<String, Format>()
  {
  @Override
  public Format apply( String input )
    {
    return new Format( input.toLowerCase() );
    }
  };

  private static final LoadingCache<String, Format> cache = makeInternedCache( factory );

  @JsonCreator
  public static Format getFormat( String name )
    {
    if( name == null || name.isEmpty() )
      return null;

    return cache.getUnchecked( name );
    }

  public static List<Format> resolveFormats( List<String> formats )
    {
    if( formats == null )
      return Collections.EMPTY_LIST;

    List<Format> results = new ArrayList<Format>();

    for( String format : formats )
      results.add( getFormat( format ) );

    return results;
    }

  private final String name;

  private Format( String name )
    {
    this.name = name;
    }

  @JsonValue
  public String getName()
    {
    return name;
    }

  @Override
  public String toString()
    {
    return name;
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

  @Override
  public int compareTo( Format format )
    {
    if( format == null )
      return Integer.MAX_VALUE;

    return this.getName().compareTo( format.getName() );
    }
  }
