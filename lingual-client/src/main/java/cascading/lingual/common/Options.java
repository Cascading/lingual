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

package cascading.lingual.common;

import java.io.IOException;
import java.io.PrintStream;

import cascading.util.Version;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Options
  {
  private static final Logger LOG = LoggerFactory.getLogger( Options.class );

  protected OptionParser parser = new OptionParser();
  protected OptionSet optionSet;

  protected OptionSpecBuilder version;
  protected OptionSpec<Void> help;
  protected OptionSpec<String> verbose;
  protected OptionSpec<String> platform;

  public Options()
    {
    help = parser.accepts( "help" ).forHelp();
    verbose = parser.accepts( "verbose" ).withOptionalArg().defaultsTo( "info" );
    version = parser.accepts( "version" );

    platform = parser.accepts( "platform", "platform planner to use" )
      .withRequiredArg().defaultsTo( "local" );
    }

  public boolean parse( PrintStream printStream, String... args ) throws IOException
    {
    try
      {
      optionSet = parser.parse( args );

      validate();
      }
    catch( Exception exception )
      {
      printInvalidOptionMessage( printStream, exception );
      }

    return optionSet != null;
    }

  public void printInvalidOptionMessage( PrintStream printStream, String message )
    {
    printStream.println( "invalid option: " + message );
    printUsage( printStream );
    }

  public void printInvalidOptionMessage( PrintStream printStream, Exception exception )
    {
    printStream.println( "invalid option: " + exception.getMessage() );
    printUsage( printStream );
    }

  protected void validate()
    {
    }

  public boolean isVersion()
    {
    return optionSet.has( version );
    }

  public boolean isHelp()
    {
    return optionSet.has( help );
    }

  public boolean isHasOptions()
    {
    return optionSet.hasOptions();
    }

  public boolean isVerbose()
    {
    return optionSet.has( verbose );
    }

  public String getVerbose()
    {
    return optionSet.valueOf( verbose );
    }

  public boolean isListPlatforms()
    {
    return isSetWithNoArg( platform );
    }

  public String getPlatform()
    {
    return optionSet.valueOf( platform );
    }

  ////////////////

  protected boolean isSetWithNoArg( OptionSpec<String> spec )
    {
    return optionSet.has( spec ) && !optionSet.hasArgument( spec );
    }

  ////////////////

  public void printDebug( PrintStream printStream )
    {
    printStream.print( "classpath: " );
    printStream.println( System.getProperty( "java.class.path" ) );
    }

  public void printUsage( PrintStream printStream )
    {
    printStream.println( String.format( "lingual [options...]" ) );
    printStream.println( "" );

    try
      {
      parser.printHelpOn( printStream );
      }
    catch( IOException exception )
      {
      LOG.error( "unable to print usage", exception );
      }
    }

  public void printVersion( PrintStream printStream )
    {
    printStream.println( cascadingVersion() );
    }

  private String cascadingVersion()
    {
    return Version.getVersionString();
    }
  }
