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

import java.io.IOException;
import java.io.PrintStream;

import cascading.lingual.jdbc.Driver;
import sqlline.SqlLine;

/**
 *
 */
public class Shell
  {
  private String[] args;
  private Options options;
  private PrintStream errPrintStream = System.err;

  public static void main( String[] args ) throws IOException
    {
    Shell shell = new Shell();

    if( !shell.parse( args ) )
      return;

    if( shell.printUsage() )
      return;

    if( shell.printVersion() )
      return;

    shell.launch();
    }

  private boolean printUsage()
    {
    if( !options.isHelp() )
      return false;

    options.printUsage( errPrintStream );

    return true;
    }

  private boolean printVersion()
    {
    if( !options.isVersion() )
      return false;

    options.printVersion( errPrintStream );

    return true;
    }

  private void launch() throws IOException
    {
    String[] sqlLineArgs = new String[]{
      "-d", Driver.class.getName(),
      "-u", options.createJDBCUrl()
    };

    SqlLine.main( sqlLineArgs );
    }

  public boolean parse( String... args ) throws IOException
    {
    this.args = args;

    // reset instance with new options
    this.options = createOptions();

    return this.options.parse( errPrintStream, args );
    }

  private Options createOptions()
    {
    return new Options();
    }
  }
