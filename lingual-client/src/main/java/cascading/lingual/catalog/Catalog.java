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

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.Properties;

import cascading.lingual.catalog.target.DDLTarget;
import cascading.lingual.catalog.target.FormatTarget;
import cascading.lingual.catalog.target.ProtocolTarget;
import cascading.lingual.catalog.target.ProviderTarget;
import cascading.lingual.catalog.target.RepoTarget;
import cascading.lingual.catalog.target.SchemaTarget;
import cascading.lingual.catalog.target.StereotypeTarget;
import cascading.lingual.catalog.target.TableTarget;
import cascading.lingual.common.Main;
import cascading.lingual.common.Target;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.platform.PlatformBrokerFactory;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Catalog extends Main<CatalogOptions>
  {
  private static final Logger LOG = LoggerFactory.getLogger( Catalog.class );
  private Properties configProperties;

  public static void main( String[] args ) throws IOException
    {
    boolean result = new Catalog().execute( args );

    if( !result )
      System.exit( -1 );
    }

  public Catalog( PrintStream outPrintStream, PrintStream errPrintStream, Properties properties )
    {
    super( outPrintStream, errPrintStream, properties );
    }

  public Catalog( PrintStream outPrintStream, PrintStream errPrintStream )
    {
    super( outPrintStream, errPrintStream );
    }

  public Catalog( Properties properties )
    {
    super( properties );
    }

  public Catalog()
    {
    }

  protected CatalogOptions createOptions()
    {
    return new CatalogOptions();
    }

  public boolean execute( String[] args ) throws IOException
    {
    if( !parse( args ) )
      return false;

    setVerbose();

    if( printUsage() )
      return true;

    if( printVersion() )
      return true;

    try
      {
      return handle();
      }
    catch( IllegalArgumentException exception )
      {
      getOptions().printInvalidOptionMessage( getErrPrintStream(), exception );
      }
    catch( IllegalStateException exception )
      {
      getOptions().printErrorMessage( getErrPrintStream(), exception );
      }
    catch( Throwable throwable )
      {
      printFailure( getErrPrintStream(), throwable );
      }

    return false;
    }

  private Properties getConfigProperties()
    {
    if( configProperties != null )
      return configProperties;

    configProperties = new Properties( getProperties() );

    for( Map.Entry<String, String> entry : getOptions().getConfig().entrySet() )
      configProperties.setProperty( entry.getKey(), Strings.emptyToNull( entry.getValue() ) );

    return configProperties;
    }

  @Override
  protected boolean handle() throws IOException
    {
    PlatformBroker platformBroker = PlatformBrokerFactory.createPlatformBroker( getOptions().getPlatform(), getConfigProperties() );
    // force a read to init dynamic enums.
    platformBroker.getCatalog();

    if( getOptions().isInit() )
      return init( platformBroker );

    if( !platformBroker.confirmMetaData() )
      {
      getPrinter().printFormatted( "path: %s has not been initialized, use --init", platformBroker.getFullMetadataPath() );
      return false;
      }

    boolean doNotWrite = false;

    try
      {
      Target target = null;

      if( getOptions().isDDL() )
        target = new DDLTarget( getPrinter(), getOptions() );
      if( getOptions().isListSchemas() || getOptions().isSchemaActions() )
        target = new SchemaTarget( getPrinter(), getOptions() );
      else if( getOptions().isListTables() || getOptions().isTableActions() )
        target = new TableTarget( getPrinter(), getOptions() );
      else if( getOptions().isListStereotypes() || getOptions().isStereotypeActions() )
        target = new StereotypeTarget( getPrinter(), getOptions() );
      else if( getOptions().isListFormats() || getOptions().isFormatActions() )
        target = new FormatTarget( getPrinter(), getOptions() );
      else if( getOptions().isListProtocols() || getOptions().isProtocolActions() )
        target = new ProtocolTarget( getPrinter(), getOptions() );
      else if( getOptions().isListProviders() || getOptions().isProviderActions() )
        target = new ProviderTarget( getPrinter(), getOptions() );
      else if( getOptions().isListRepos() || getOptions().isRepoActions() )
        target = new RepoTarget( getPrinter(), getOptions() );

      if( target == null )
        return getOptions().printInvalidOptionMessage( getErrPrintStream(), "no command given: missing --add, --rename, --remove, --update, --validate, --show" );

      if( !( target instanceof ProtocolTarget || target instanceof FormatTarget ) && getOptions().hasProperties() )
        return getOptions().printInvalidOptionMessage( getErrPrintStream(), "--properties may only be added to formats or protocols via --add or --update" );

      return target.handle( platformBroker );
      }
    catch( Throwable throwable )
      {
      doNotWrite = true;
      Throwables.propagate( throwable );
      return false;
      }
    finally
      {
      LOG.info( "catalog loaded: {}", platformBroker.catalogLoaded() );

      if( !doNotWrite && platformBroker.catalogLoaded() )
        platformBroker.writeCatalog();
      }
    }

  private boolean init( PlatformBroker platformBroker )
    {
    LOG.debug( "catalog: init" );

    boolean success = platformBroker.initializeMetaData();

    if( success )
      platformBroker.writeCatalog();

    if( !success )
      getPrinter().printFormatted( "path: %s has already been initialized", platformBroker.getFullMetadataPath() );
    else
      getPrinter().printFormatted( "path: %s has been initialized", platformBroker.getFullMetadataPath() );

    return success;
    }
  }
