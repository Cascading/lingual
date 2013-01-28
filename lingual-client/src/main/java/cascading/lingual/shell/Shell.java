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

package cascading.lingual.shell;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cascading.lingual.common.Main;
import cascading.lingual.jdbc.Driver;
import sqlline.SqlLine;

/**
 *
 */
public class Shell extends Main<ShellOptions>
  {
  public static void main( String[] args ) throws IOException
    {
    boolean result = new Shell().execute( args );

    if( !result )
      {
      System.exit( -1 );
      }
    }

  public boolean execute( String[] args ) throws IOException
    {
    if( !parse( args ) )
      {
      return true;
      }

    setVerbose();

    if( printUsage() )
      {
      return true;
      }

    if( printVersion() )
      {
      return true;
      }

    try
      {
      return handle();
      }
    catch( IllegalArgumentException exception )
      {
      getOptions().printInvalidOptionMessage( getErrPrintStream(), exception );
      }
    catch( Throwable throwable )
      {
      printFailure( getErrPrintStream(), throwable );
      }

    return false;
    }

  protected ShellOptions createOptions()
    {
    return new ShellOptions();
    }

  @Override
  protected boolean handle() throws IOException
    {
//    cmd-usage: Usage: java sqlline.SqlLine \n \
//    \  -u <database url>               the JDBC URL to connect to\n \
//    \  -n <username>                   the username to connect as\n \
//    \  -p <password>                   the password to connect as\n \
//    \  -d <driver class>               the driver class to use\n \
//    \  --color=[true/false]            control whether color is used for display\n \
//    \  --showHeader=[true/false]       show column names in query results\n \
//    \  --headerInterval=ROWS;          the interval between which heades are displayed\n \
//    \  --fastConnect=[true/false]      skip building table/column list for tab-completion\n \
//    \  --autoCommit=[true/false]       enable/disable automatic transaction commit\n \
//    \  --verbose=[true/false]          show verbose error messages and debug info\n \
//    \  --showWarnings=[true/false]     display connection warnings\n \
//    \  --force=[true/false]            continue running script even after errors\n \
//    \  --maxWidth=MAXWIDTH             the maximum width of the terminal\n \
//    \  --maxColumnWidth=MAXCOLWIDTH    the maximum width to use when displaying columns\n \
//    \  --silent=[true/false]           be more silent\n \
//    \  --autosave=[true/false]         automatically save preferences\n \
//    \  --outputformat=[table/vertical/csv/tsv]   format mode for result display\n \
//    \  --isolation=LEVEL               set the transaction isolation level\n \
//    \  --help                          display this message

    List<String> args = new ArrayList<String>();

    add( args, "-d", Driver.class.getName() );
    add( args, "-u", getOptions().createJDBCUrl() );

    if( getOptions().isVerbose() )
      {
      add( args, "--verbose", "true" );
      }

    if( getOptions().getSqlFile() != null )
      {
      add( args, "--autoCommit", "false" );
      }

    String[] sqlLineArgs = args.toArray( new String[ args.size() ] );

    String sql = getOptions().getSqlFile();

    if( sql == null )
      {
      SqlLine.main( sqlLineArgs );
      }
    else if( "-" .equals( sql ) )
      {
      SqlLine.mainWithInputRedirection( sqlLineArgs, System.in );
      }
    else
      {
      SqlLine.mainWithInputRedirection( sqlLineArgs, new FileInputStream( sql ) );
      }

    return true;
    }

  private void add( List<String> args, String string, String name )
    {
    args.add( string );
    args.add( name );
    }
  }
