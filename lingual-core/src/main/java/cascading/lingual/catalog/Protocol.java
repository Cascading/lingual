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
import java.util.List;

import cascading.lingual.util.Util;
import com.google.common.base.Function;
import com.google.common.cache.LoadingCache;

/**
 *
 */
public class Protocol implements Serializable
  {
  private static final Function<String, Protocol> factory = new Function<String, Protocol>()
  {
  @Override
  public Protocol apply( String input )
    {
    return new Protocol( input.toLowerCase() );
    }
  };

  private static final LoadingCache<String, Protocol> cache = Util.makeInternedCache( factory );

  public static Protocol getProtocol( String name )
    {
    if( name == null || name.isEmpty() )
      return null;

    return cache.getUnchecked( name );
    }

  public static List<Protocol> resolveProtocols( List<String> protocols )
    {
    List<Protocol> results = new ArrayList<Protocol>();

    for( String protocol : protocols )
      results.add( getProtocol( protocol ) );

    return results;
    }

  private final String name;

  protected Protocol( String name )
    {
    this.name = name;
    }

  public String getName()
    {
    return name;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( "Protocol" );
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

    Protocol protocol = (Protocol) object;

    if( name != null ? !name.equals( protocol.name ) : protocol.name != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    return name != null ? name.hashCode() : 0;
    }
  }
