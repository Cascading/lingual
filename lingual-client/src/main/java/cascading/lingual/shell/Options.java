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
import java.util.List;

import cascading.util.Util;
import cascading.util.Version;
import joptsimple.ArgumentAcceptingOptionSpec;
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

  OptionParser parser = new OptionParser();

  private OptionSet optionSet;

  private OptionSpecBuilder version;
  private OptionSpecBuilder help;

  private OptionSpec<String> platform;
  private ArgumentAcceptingOptionSpec<String> schemas;

  private OptionSpec<String> resultPath;
  private OptionSpec<String> dotPath;

  public Options()
    {
    version = parser.accepts( "version" );
    help = parser.accepts( "help" );

    platform = parser.accepts( "platform", "platform planner to use" )
      .withRequiredArg().defaultsTo( "local" );

    schemas = parser.accepts( "schemas", "root path for each schema to use" )
      .withRequiredArg().withValuesSeparatedBy( ',' );

    resultPath = parser.accepts( "resultPath", "root temp path to store results" )
      .withRequiredArg();

    dotPath = parser.accepts( "dotPath", "path to write flow dot files" )
      .withRequiredArg();
    }

  public boolean parse( PrintStream printStream, String... args ) throws IOException
    {
    try
      {
      optionSet = parser.parse( args );
      }
    catch( Exception exception )
      {
      printStream.println( "invalid option: " + exception.getMessage() );
      printUsage( printStream );
      }

    return optionSet != null;
    }

  public String createJDBCUrl()
    {
    StringBuilder builder = new StringBuilder( "jdbc:lingual:" );

    builder.append( getPlatform() );

    if( !getSchemas().isEmpty() )
      {
      builder
        .append( ";schemas=" )
        .append( Util.join( getSchemas(), "," ) );
      }

    if( getResultPath() != null )
      builder.append( ";resultPath=" ).append( getResultPath() );

    if( getDotPath() != null )
      builder.append( ";dotPath=" ).append( getDotPath() );

    return builder.toString();
    }

  /////

  public boolean isVersion()
    {
    return optionSet.has( version );
    }

  public boolean isHelp()
    {
    return optionSet.has( help );
    }

  public String getPlatform()
    {
    return optionSet.valueOf( platform );
    }

  public List<String> getSchemas()
    {
    return optionSet.valuesOf( schemas );
    }

  public String getResultPath()
    {
    return optionSet.valueOf( resultPath );
    }

  public String getDotPath()
    {
    return optionSet.valueOf( dotPath );
    }

  /////

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
