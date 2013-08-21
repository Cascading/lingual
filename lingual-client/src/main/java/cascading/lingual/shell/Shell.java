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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import cascading.lingual.common.Main;
import cascading.lingual.jdbc.Driver;
import cascading.lingual.util.Version;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sqlline.SqlLine;

import static java.util.Collections.addAll;

/**
 *
 */
public class Shell extends Main<ShellOptions>
  {
  private static final Logger LOG = LoggerFactory.getLogger( Shell.class );

  InputStream inputStream = System.in;

  public Shell()
    {
    }

  public Shell( InputStream inputStream, PrintStream outPrintStream, PrintStream errPrintStream, Properties properties )
    {
    super( outPrintStream, errPrintStream, properties );
    this.inputStream = inputStream;
    }

  public Shell( PrintStream outPrintStream, PrintStream errPrintStream, Properties properties )
    {
    super( outPrintStream, errPrintStream, properties );
    }

  public static void main( String[] args ) throws IOException
    {
    boolean result = new Shell().execute( args );

    if( !result )
      System.exit( -1 );
    }

  public boolean execute( String[] args ) throws IOException
    {
    if( !parse( args ) )
      return true;

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

    getPrinter().printFormatted( Version.getBannerVersionString() );

    // sqlline doesn't handle basic dir config from command line but it does handle it from System.
    String sqlLineDir = Joiner.on( File.separator ).join( System.getProperty( "user.home" ), ".lingual", "sqlline" );
    System.setProperty( SqlLine.SQLLINE_BASE_DIR, sqlLineDir );

    List<String> args = new ArrayList<String>();

    addAll( args, "-d", Driver.class.getName() );
    addAll( args, "-u", getOptions().createJDBCUrl( properties ) );
    addAll( args, "--headerInterval=100" ); // 100 is default

    if( getOptions().hasUsername() )
      addAll( args, "-n", getOptions().getUsername() );

    if( getOptions().hasPassword() )
      addAll( args, "-p", getOptions().getPassword() );

    // this breaks !tables etc,
    // but is required if we are going to output 10B rows
    if( getOptions().getMaxRows() == null || getOptions().getMaxRows() == 0 )
      {
      addAll( args, "--incremental=true" );
      }
    else
      {
      addAll( args, "--incremental=false" ); // allows buffering
      getPrinter().printFormatted( "only %,d rows will be displayed", getOptions().getMaxRows() );
      }

    if( getOptions().isVerbose() )
      addAll( args, "--verbose=true" );

    if( getOptions().getSqlFile() != null )
      addAll( args, "--autoCommit=false" );

    String[] sqlLineArgs = args.toArray( new String[ args.size() ] );

    LOG.info( "sqlline args: {}", Arrays.toString( sqlLineArgs ) );

    String sql = getOptions().getSqlFile();

    boolean result = false;
    if( sql == null )
      {
      result = true; // interactive use assumes interactive validation.
      LOG.info( "starting shell" );
      SqlLine.main( sqlLineArgs );
      }
    else if( "-".equals( sql ) )
      {
      LOG.info( "reading from stdin" );
      result = SqlLine.mainWithInputRedirection( sqlLineArgs, inputStream );
      }
    else
      {
      LOG.info( "reading from {}", sql );
      String runCommand = SqlLine.COMMAND_PREFIX + "run " + sql + "\n";
      InputStream commandStream = new ByteArrayInputStream( runCommand.getBytes() );
      try
        {
        result = SqlLine.mainWithInputRedirection( sqlLineArgs, commandStream );
        }
      finally
        {
        commandStream.close();
        }
      }

    return result;
    }
  }
