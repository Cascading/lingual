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

package cascading.lingual.catalog.provider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.SchemaProperties;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import static com.google.common.collect.Lists.newCopyOnWriteArrayList;
import static java.util.Arrays.asList;

/**
 *
 */
public class ProviderDefinition
  {
  public static final String CASCADING_BIND_PROVIDER_PROPERTIES = "cascading/bind/provider.properties";

  public static final String NAMES = "names";
  public static final String PROVIDER_BASE = "cascading.bind.provider.";
  public static final String PROVIDER_NAMED_BASE = "cascading.bind.provider.%s.";
  public static final String PROVIDER_NAME = PROVIDER_BASE + NAMES;
  public static final String PROVIDER_FACTORY_CLASS_NAME = PROVIDER_BASE + "%s.factory.classname";
  public static final String PROVIDER_EXTENDS = PROVIDER_BASE + "%s.extends";
  public static final String PROVIDER_PLATFORMS = PROVIDER_BASE + "%s.platforms";
  public static final String PROVIDER_PROTOCOL = PROVIDER_BASE + "%s.protocol.";
  public static final String PROVIDER_FORMAT = PROVIDER_BASE + "%s.format.";
  public static final String PROVIDER_STEREOTYPE = PROVIDER_BASE + "%s.stereotype.";
  public static final String PROVIDER_DESCRIPTION = PROVIDER_BASE + "%s.description";

  private final String providerName;
  private Map<String, String> properties;

  public static ProviderDefinition[] getProviderDefinitions( Properties set )
    {
    return getProviderDefinitions( Maps.fromProperties( set ) );
    }

  public static ProviderDefinition[] getProviderDefinitions( Map<String, String> definitions )
    {
    String[] names = getNames( definitions );
    ProviderDefinition[] providerDefinitions = new ProviderDefinition[ names.length ];

    for( int i = 0; i < names.length; i++ )
      providerDefinitions[ i ] = new ProviderDefinition( names[ i ], definitions );

    return providerDefinitions;
    }

  public static String[] getNames( Map<String, String> definitions )
    {
    String names = definitions.get( PROVIDER_NAME );

    if( names == null )
      return new String[ 0 ];

    return names.split( "," );
    }

  public ProviderDefinition( String providerName, Map<String, String> properties )
    {
    this.providerName = providerName;
    this.properties = new HashMap<String, String>( properties );
    }

  public Map<String, String> getProperties()
    {
    return properties;
    }

  public String getProviderName()
    {
    return providerName;
    }

  public Map<String, String> getProviderPropertyMap()
    {
    return filterMap( property( PROVIDER_NAMED_BASE ) );
    }

  public String getDescription()
    {
    return properties.get( property( PROVIDER_DESCRIPTION ) );
    }

  private String property( String property )
    {
    return String.format( property, providerName );
    }

  public List<String> getPlatforms()
    {
    String platforms = properties.get( property( PROVIDER_PLATFORMS ) );

    if( platforms == null )
      return Collections.EMPTY_LIST;

    return newCopyOnWriteArrayList( asList( platforms.split( "," ) ) );
    }

  public String getFactoryClassName()
    {
    return properties.get( property( PROVIDER_FACTORY_CLASS_NAME ) );
    }

  public String getExtends()
    {
    return properties.get( property( PROVIDER_EXTENDS ) );
    }

  public Map<Protocol, Map<String, List<String>>> getDefaultProtocolProperties()
    {
    Map<Protocol, Map<String, List<String>>> map = new HashMap<Protocol, Map<String, List<String>>>();
    String strings = properties.get( property( PROVIDER_PROTOCOL ) + NAMES );
    List<String> protocols = ( strings == null ) ? Collections.<String>emptyList() : asList( strings.split( "," ) );

    for( String protocol : protocols )
      {
      Map<String, List<String>> properties = parse( property( PROVIDER_PROTOCOL ), protocol );

      String providerName = getExtends();

      if( providerName == null )
        providerName = getProviderName();

      properties.put( SchemaProperties.PROVIDER, newCopyOnWriteArrayList( asList( providerName ) ) );
      map.put( Protocol.getProtocol( protocol ), properties );
      }

    return map;
    }

  public Map<Format, Map<String, List<String>>> getDefaultFormatProperties()
    {
    Map<Format, Map<String, List<String>>> map = new HashMap<Format, Map<String, List<String>>>();
    String strings = properties.get( property( PROVIDER_FORMAT ) + NAMES );
    List<String> formats = ( strings == null ) ? Collections.<String>emptyList() : asList( strings.split( "," ) );

    for( String formatName : formats )
      {
      Map<String, List<String>> properties = parse( property( PROVIDER_FORMAT ), formatName );

      String providerName = getExtends();

      if( providerName == null )
        providerName = getProviderName();

      properties.put( SchemaProperties.PROVIDER, newCopyOnWriteArrayList( asList( providerName ) ) );
      map.put( Format.getFormat( formatName ), properties );
      }

    return map;
    }

  private Map<String, List<String>> parse( String keyPrefix, String element )
    {
    String prefix = keyPrefix + element + ".";
    int length = prefix.length();
    Map<String, String> map = filterMap( prefix );

    Map<String, List<String>> result = new HashMap<String, List<String>>();

    for( Map.Entry<String, String> entry : map.entrySet() )
      {
      String value = entry.getValue();

      if( Strings.isNullOrEmpty( value ) )
        result.put( entry.getKey().substring( length ), new ArrayList<String>() );
      else if( value.equals( "," ) )
        result.put( entry.getKey().substring( length ), newCopyOnWriteArrayList( asList( value ) ) );
      else
        result.put( entry.getKey().substring( length ), newCopyOnWriteArrayList( asList( value.split( "," ) ) ) );
      }

    return result;
    }

  private Map<String, String> filterMap( final String prefix )
    {
    return Maps.filterKeys( properties, new Predicate<String>()
    {
    @Override
    public boolean apply( String input )
      {
      return input.startsWith( prefix );
      }
    } );
    }
  }
