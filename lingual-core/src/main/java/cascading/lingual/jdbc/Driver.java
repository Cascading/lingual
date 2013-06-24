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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import cascading.lingual.optiq.EnumerableTapRule;
import cascading.lingual.util.Logging;
import cascading.lingual.util.Version;
import net.hydromatic.linq4j.function.Function0;
import net.hydromatic.optiq.jdbc.DriverVersion;
import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.jdbc.UnregisteredDriver;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.rules.java.JavaRules;
import org.eigenbase.rel.rules.ProjectToCalcRule;
import org.eigenbase.rel.rules.TableAccessRule;
import org.eigenbase.relopt.ConventionTraitDef;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.volcano.VolcanoPlanner;
import org.eigenbase.util14.ConnectStringParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lingual JDBC driver.
 * <p/>
 * The JDBC Driver connection string consists of the following parts:
 * <p/>
 * {@code jdbc:lingual:[platform]}
 * <p/>
 * Optionally, the following parameters can be added
 * <p/>
 * <ul>
 * <li>{@code catalog=[path]} - the working directory where your .lingual workspace is kept</li>
 * <li>{@code schema=[name]} - set the default schema name to use</li>
 * <li>{@code schemas=[path,path]} - URI paths to the set of schema/tables to install in the catalog</li>
 * <li>{@code resultPath=[path]} - temporary root path for result sets to be stored</li>
 * <li>{@code flowPlanPath=[path]} - for debugging, print the corresponding Flow dot file here</li>
 * <li>{@code sqlPlanPath=[path]} - for debugging, print the corresponding SQL plan file here</li>
 * </ul>
 * <p/>
 * For example, using Cascading local mode, and to load all the subdirectories of {@code ./employees/} as tables,
 * use the following connection string:
 * <p/>
 * {@code jdbc:lingual:local;schemas=./employees/}
 * <p/>
 * This will create a schema called "employees" and every file underneath (files as opposed to directories on Hadoop)
 * will become tables named after the base-name of the file (minus the extension).
 */
public class Driver extends UnregisteredDriver
  {
  private static final Logger LOG = LoggerFactory.getLogger( Driver.class );

  private final Factory factory = instantiateFactory();

  public static final String PLATFORM_PROP = "platform";
  public static final String CATALOG_PROP = "catalog";
  public static final String SCHEMA_PROP = "schema";
  public static final String SCHEMAS_PROP = "schemas";
  public static final String TABLES_PROP = "tables";
  public static final String VERBOSE_PROP = "verbose";
  public static final String RESULT_PATH_PROP = "resultPath";
  public static final String MAX_ROWS = "maxRows";
  public static final String FLOW_PLAN_PATH = "flowPlanPath";
  public static final String SQL_PLAN_PATH_PROP = "sqlPlanPath";
  public static final String COLLECTOR_CACHE_PROP = "collectorCache";
  public static final String PLANNER_DEBUG = "plannerDebug";

  static
    {
    new Driver().register();
    }

  @Override
  protected String getConnectStringPrefix()
    {
    return "jdbc:lingual:";
    }

  @Override
  protected void register()
    {
    try
      {
      DriverManager.registerDriver( this );
      }
    catch( SQLException exception )
      {
      LOG.error( "error occurred while registering JDBC driver " + this + ": " + exception.toString() );
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
  protected Function0<OptiqPrepare> createPrepareFactory()
    {
    return new Function0<OptiqPrepare>()
    {
    public OptiqPrepare apply()
      {
      return new LingualPrepare();
      }
    };
    }

  @Override
  public Connection connect( String url, Properties info ) throws SQLException
    {
    if( !acceptsURL( url ) )
      {
      LOG.error( "invalid connection url {}", url );
      return null;
      }

    Connection connection = super.connect( url, info );

    if( connection == null )
      {
      LOG.error( "unable to get connection to {} with {}", url, info );
      return null;
      }

    Properties connectionProperties = parseConnectionProperties( url, info );

    if( connectionProperties.contains( VERBOSE_PROP ) )
      Logging.setLogLevel( connectionProperties.getProperty( VERBOSE_PROP, "info" ) );

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
    try
      {
      return new JaninoFactory();
      }
    catch( Throwable e )
      {
      LOG.error( "Error while instantiating driver factory", e );
      return null;
      }
    }

  /** Refine the query-preparation process for Lingual. */
  private static class LingualPrepare extends OptiqPrepareImpl
    {
    @Override
    protected List<Function0<RelOptPlanner>> createPlannerFactories()
      {
      return Arrays.asList(
        new Function0<RelOptPlanner>()
        {
        public RelOptPlanner apply()
          {
          return createTapPlanner();
          }
        },
        new Function0<RelOptPlanner>()
        {
        public RelOptPlanner apply()
          {
          return createPlanner();
          }
        }
      );
      }

    /**
     * Creates a simple planner that can plan "select * from myTable" but not
     * much more.
     */
    protected RelOptPlanner createTapPlanner()
      {
      final VolcanoPlanner planner = new VolcanoPlanner();
      planner.addRelTraitDef( ConventionTraitDef.instance );
      planner.addRule( TableAccessRule.instance );
      planner.addRule( JavaRules.ENUMERABLE_CALC_RULE );
      planner.addRule( ProjectToCalcRule.instance );
      planner.addRule( EnumerableTapRule.INSTANCE );
      planner.setLocked( true ); // prevent further rules being added
      return planner;
      }
    }
  }
