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

package cascading.lingual.jdbc;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import cascading.lingual.util.Version;
import net.hydromatic.optiq.jdbc.DriverVersion;
import net.hydromatic.optiq.jdbc.UnregisteredDriver;
import org.eigenbase.util14.ConnectStringParser;

/** Lingual JDBC driver. */
public class Driver extends UnregisteredDriver
  {
  private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger( Driver.class );
  private final Factory factory = instantiateFactory();

  public static final String PLATFORM_PROP = "platform";
  public static final String CATALOG_PROP = "catalog";
  public static final String SCHEMA_PROP = "schema";
  public static final String SCHEMAS_PROP = "schemas";
  public static final String TABLES_PROP = "tables";
  public static final String RESULT_PATH_PROP = "resultPath";
  public static final String DOT_PATH_PROP = "dotPath";

  static
    {
    new Driver().register();
    }

  @Override
  protected String getConnectStringPrefix()
    {
    return "jdbc:lingual:";
    }

  protected void register()
    {
    try
      {
      DriverManager.registerDriver( this );
      }
    catch( SQLException exception )
      {
      LOG.error( "Error occurred while registering JDBC driver " + this + ": " + exception.toString() );
      }
    }

  @Override
  protected DriverVersion createDriverVersion()
    {
    return new DriverVersion(
      Version.getName(),
      Version.getVersionString(),
      Version.getProductName(),
      Version.getProductVersion(),
      true,
      Version.getMajorVersion(),
      Version.getMinorVersion(),
      1,
      0
    );
    }

  @Override
  public Connection connect( String url, Properties info ) throws SQLException
    {
    Connection connection = super.connect( url, info );

    if( connection == null )
      return null;

    Properties connectionProperties = parseConnectionProperties( url, info );

    return factory.createConnection( connection, connectionProperties );
    }

  private Properties parseConnectionProperties( String url, Properties info ) throws SQLException
    {
    String urlSuffix = getSuffix( url, info );

    return ConnectStringParser.parse( urlSuffix, info );
    }

  private String getSuffix( String url, Properties info )
    {
    String urlSuffix = url.substring( getConnectStringPrefix().length() );

    String[] parts = urlSuffix.split( ";" );

    if( !parts[ 0 ].contains( "=" ) )
      {
      String[] elements = parts[ 0 ].split( ":" );

      info.put( PLATFORM_PROP, elements[ 0 ] );

      if( elements.length == 2 )
        info.put( SCHEMA_PROP, elements[ 1 ] );

      if( urlSuffix.length() > parts[ 0 ].length() )
        urlSuffix = urlSuffix.substring( parts[ 0 ].length() + 1 );
      }

    return urlSuffix;
    }

  static Factory instantiateFactory()
    {
    if( true )
      return new JaninoFactory();

    try
      {
      Class clazz = Class.forName( factoryClassName() );
      return (Factory) clazz.newInstance();
      }
    catch( Throwable e )
      {
      LOG.error( "Error while instantiating driver factory", e );
      return null;
      }
    }

  static String factoryClassName()
    {
    return "cascading.lingual.jdbc.FactoryJdbc40";
    }
  }
