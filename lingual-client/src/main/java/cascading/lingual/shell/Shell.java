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

package cascading.lingual.shell;

import java.io.FileInputStream;
import java.io.IOException;

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
    String[] sqlLineArgs = new String[]{
      "-d", Driver.class.getName(),
      "-u", getOptions().createJDBCUrl()
    };

    String sql = getOptions().getSqlFile();

    if( sql == null )
      SqlLine.main( sqlLineArgs );
    else if( "-".equals( sql ) )
      SqlLine.mainWithInputRedirection( sqlLineArgs, System.in );
    else
      SqlLine.mainWithInputRedirection( sqlLineArgs, new FileInputStream( sql ) );

    return true;
    }
  }
