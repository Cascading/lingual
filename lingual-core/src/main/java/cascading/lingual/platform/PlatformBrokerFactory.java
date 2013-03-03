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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class PlatformBrokerFactory
  {
  private static final Logger LOG = LoggerFactory.getLogger( PlatformBrokerFactory.class );

  public static final String PLATFORM_NAME = "platform.name";
  public static final String PLATFORM_INCLUDES = "platform.includes";
  public static final String PLATFORM_RESOURCE = "cascading/lingual/platform.properties";
  public static final String PLATFORM_CLASSNAME = "platform.broker.classname";

  private Set<String> includes = new HashSet<String>();

  private Map<String, PlatformBroker> brokers = new HashMap<String, PlatformBroker>();

  static PlatformBrokerFactory factory;

  public static synchronized PlatformBrokerFactory instance()
    {
    if( factory == null )
      factory = new PlatformBrokerFactory();

    return factory;
    }

  public static PlatformBroker createPlatformBroker( String platformName, Properties properties )
    {
    return instance().getPlatformBroker( platformName, properties );
    }

  public PlatformBrokerFactory()
    {
    setIncludes();
    loadBrokers();
    }

  private PlatformBroker getPlatformBroker( String platform, Properties properties )
    {
    // override platform from properties
    platform = properties.getProperty( PLATFORM_NAME, platform );

    PlatformBroker broker = brokers.get( platform.toLowerCase() );

    if( broker == null )
      throw new IllegalArgumentException( "platform broker not found for: " + platform );

    broker.setProperties( properties );

    return broker;
    }

  private void setIncludes()
    {
    String includesString = System.getProperty( PLATFORM_INCLUDES );

    if( includesString == null || includesString.isEmpty() )
      return;

    String[] split = includesString.split( "," );

    for( String include : split )
      includes.add( include.trim().toLowerCase() );
    }

  /** for testing */
  public void reloadBrokers()
    {
    LOG.debug( "reloading brokers" );
    brokers.clear();
    loadBrokers();
    }

  private void loadBrokers()
    {
    ClassLoader contextClassLoader = getClass().getClassLoader();
    Set<Class<? extends PlatformBroker>> classes = getPlatformClass( contextClassLoader );

    for( Class<? extends PlatformBroker> platformClass : classes )
      addPlatform( platformClass );
    }

  private void addPlatform( Class<? extends PlatformBroker> type )
    {
    final PlatformBroker broker = makeInstance( type );

    // test platform dependencies not installed, so skip
    if( broker == null )
      return;

    final String platformName = broker.getName().toLowerCase();

    if( !includes.isEmpty() && !includes.contains( platformName ) )
      {
      LOG.info( "ignoring platform: {}", platformName );
      return;
      }

    LOG.info( "installing platform: {}", platformName );

    brokers.put( platformName, broker );
    }

  private static Class<?> getPlatformClass( ClassLoader classLoader, Properties properties, InputStream stream ) throws IOException, ClassNotFoundException
    {
    if( stream == null )
      throw new IllegalStateException( "platform provider resource not found: " + PLATFORM_RESOURCE );

    properties.load( stream );

    String classname = properties.getProperty( PLATFORM_CLASSNAME );

    if( classname == null )
      throw new IllegalStateException( "platform provider value not found: " + PLATFORM_CLASSNAME );

    Class<?> type = classLoader.loadClass( classname );

    if( type == null )
      throw new IllegalStateException( "platform provider class not found: " + classname );

    return type;
    }

  public static Set<Class<? extends PlatformBroker>> getPlatformClass( ClassLoader classLoader )
    {
    Set<Class<? extends PlatformBroker>> classes = new HashSet<Class<? extends PlatformBroker>>();
    Properties properties = new Properties();

    LOG.debug( "classloader: {}", classLoader );

    Enumeration<URL> urls;

    try
      {
      urls = classLoader.getResources( PLATFORM_RESOURCE );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to load resources" );
      }

    while( urls.hasMoreElements() )
      addClasses( classLoader, classes, properties, urls.nextElement() );

    return classes;
    }

  private static void addClasses( ClassLoader classLoader, Set<Class<? extends PlatformBroker>> classes, Properties properties, URL url )
    {
    try
      {
      InputStream stream = url.openStream();
      classes.add( (Class<? extends PlatformBroker>) getPlatformClass( classLoader, properties, stream ) );
      }
    catch( Exception exception )
      {
      throw new RuntimeException( "unable to load platform class from: " + url );
      }
    }

  public static PlatformBroker makeInstance( Class<? extends PlatformBroker> type )
    {
    try
      {
      return type.newInstance();
      }
    catch( NoClassDefFoundError exception )
      {
      LOG.error( "failed loading platform", exception );
      return null;
      }
    catch( InstantiationException exception )
      {
      throw new RuntimeException( exception );
      }
    catch( IllegalAccessException exception )
      {
      throw new RuntimeException( exception );
      }
    }
  }
