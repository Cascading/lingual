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

import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

import cascading.lingual.common.Main;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.platform.PlatformBrokerFactory;

/**
 *
 */
public class Catalog extends Main<CatalogOptions>
  {
  Properties properties;

  public static void main( String[] args ) throws IOException
    {
    new Catalog().execute( args );
    }

  public Catalog( PrintStream outPrintStream, PrintStream errPrintStream, Properties properties )
    {
    super( outPrintStream, errPrintStream );
    this.properties = properties;
    }

  public Catalog( PrintStream outPrintStream, PrintStream errPrintStream )
    {
    super( outPrintStream, errPrintStream );
    }

  public Catalog( Properties properties )
    {
    this.properties = properties;
    }

  public Catalog()
    {
    }

  protected CatalogOptions createOptions()
    {
    return new CatalogOptions();
    }

  public void execute( String[] args ) throws IOException
    {
    if( !parse( args ) )
      return;

    if( printUsage() )
      return;

    if( printVersion() )
      return;

    handle();
    }

  @Override
  protected boolean handle() throws IOException
    {
    PlatformBroker platformBroker = PlatformBrokerFactory.createPlatformBroker( getOptions().getPlatform(), properties );

    if( getOptions().isInit() )
      return metaDataPath( platformBroker );

    try
      {
      if( getOptions().isListSchemas() || getOptions().isSchemaActions() )
        return handleSchema( platformBroker );
      else if( getOptions().isListTables() || getOptions().isTableActions() )
        return handleTable( platformBroker );
      else if( getOptions().isListFormats() || getOptions().isFormatActions() )
        return handleFormat( platformBroker );
      else if( getOptions().isListProtocols() || getOptions().isProtocolActions() )
        return handleProtocol( platformBroker );

      throw new RuntimeException( "no command executed" );
      }
    finally
      {
      platformBroker.writeCatalog();
      }
    }

  private boolean handleSchema( PlatformBroker platformBroker )
    {
    return new SchemaHandler( getPrinter(), getOptions() ).handle( platformBroker );
    }

  private boolean handleTable( PlatformBroker platformBroker )
    {
    return new TableHandler( getPrinter(), getOptions() ).handle( platformBroker );
    }

  private boolean handleFormat( PlatformBroker platformBroker )
    {
    return new FormatHandler( getPrinter(), getOptions() ).handle( platformBroker );
    }

  protected boolean handleProtocol( PlatformBroker platformBroker )
    {
    return new ProtocolHandler( getPrinter(), getOptions() ).handle( platformBroker );
    }

  private boolean metaDataPath( PlatformBroker platformBroker )
    {
    boolean result = platformBroker.initializeMetaData();

    if( result )
      getPrinter().print( "path: %s has already been initialized", platformBroker.getFullMetadataPath() );
    else
      getPrinter().print( "path: %s has been initialized", platformBroker.getFullMetadataPath() );

    return !result;
    }
  }
