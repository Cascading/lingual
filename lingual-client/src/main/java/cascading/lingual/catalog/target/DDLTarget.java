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

package cascading.lingual.catalog.target;

import java.io.File;
import java.io.IOException;
import java.util.List;

import cascading.lingual.catalog.CatalogOptions;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.ddl.DDLParser;
import cascading.lingual.catalog.ddl.DDLTable;
import cascading.lingual.common.Printer;
import cascading.lingual.common.Target;
import cascading.lingual.platform.PlatformBroker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class DDLTarget extends Target
  {
  private static final Logger LOG = LoggerFactory.getLogger( DDLTarget.class );

  public DDLTarget( Printer printer, CatalogOptions options )
    {
    super( printer, options );
    }

  @Override
  public boolean handle( PlatformBroker platformBroker )
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    String schemaName = getOptions().getSchemaName();
    String protocolName = getOptions().getProtocolName();
    String formatName = getOptions().getFormatName();

    verifySchema( catalog, schemaName );

    LOG.info( "loading ddl from: {}", getOptions().getDDL() );

    DDLParser parser = new DDLParser( catalog, schemaName, protocolName, formatName );

    try
      {
      List<DDLTable> commands = parser.parse( new File( getOptions().getDDL() ) );

      LOG.info( "found {} commands", commands.size() );

      parser.execute( commands );
      }
    catch( IOException exception )
      {
      throw new IllegalArgumentException( "unable to read file: " + getOptions().getDDL(), exception );
      }

    return true;
    }
  }
