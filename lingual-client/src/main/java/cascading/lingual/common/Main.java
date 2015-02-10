/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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
import java.util.Properties;
import java.util.logging.Level;

import cascading.lingual.util.Eigenbase;
import com.google.common.base.Throwables;
import org.eigenbase.trace.EigenbaseTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.lingual.util.Logging.setLogLevel;

/**
 * Super class of all commandline tools in Lingual.
 */
public abstract class Main<O extends Options>
  {
  private static final Logger LOG = LoggerFactory.getLogger( Main.class );

  protected final PrintStream outPrintStream;
  protected final PrintStream errPrintStream;
  protected final Properties properties;

  private String[] args;

  private O options;

  protected Main()
    {
    this( System.out, System.err, new Properties() );
    }

  public Main( Properties properties )
    {
    this( System.out, System.err, properties );
    }

  protected Main( PrintStream outPrintStream, PrintStream errPrintStream )
    {
    this( outPrintStream, errPrintStream, new Properties(  ) );
    }

  protected Main( PrintStream outPrintStream, PrintStream errPrintStream, Properties properties )
    {
    this.outPrintStream = outPrintStream;
    this.errPrintStream = errPrintStream;
    this.properties = sanitize( properties );
    }

  private Properties sanitize( Properties properties )
    {
    for ( String key: properties.stringPropertyNames() )
      {
      String value = properties.getProperty( key );
      if( value.contains( " " ) )
        {
        LOG.warn( "Removing spaces from value '{}' for key '{}'.", key, value );
        properties.setProperty( key, value.replaceAll( " ", "" ) );
        }

      }
    return properties;
    }

  public O getOptions()
    {
    return options;
    }

  public Properties getProperties()
    {
    return properties;
    }

  public Printer getPrinter()
    {
    return new Printer( getOutPrintStream() );
    }

  protected boolean printUsage()
    {
    if( !options.isHelp() )
      return false;

    options.printUsage( getOutPrintStream() );

    return true;
    }

  protected boolean printVersion()
    {
    if( !options.isVersion() )
      return false;

    options.printVersion( getOutPrintStream() );

    return true;
    }

  protected abstract boolean handle() throws IOException;

  public boolean parse( String... args ) throws IOException
    {
    this.args = args;

    // reset instance with new options
    this.options = createOptions();

    return this.options.parse( getErrPrintStream(), args );
    }

  protected abstract O createOptions();

  public PrintStream getOutPrintStream()
    {
    return outPrintStream;
    }

  public PrintStream getErrPrintStream()
    {
    return errPrintStream;
    }

  protected void setVerbosity()
    {
    if( getOptions().isVerbose() )
      {
      setLogLevel( Main.class.getClassLoader(), "", getOptions().getVerbose() );
      EigenbaseTrace.getPlannerTracer().setLevel( Level.SEVERE );
      }
    if( getOptions().isShowStackTrace() )
      {
      setLogLevel( Main.class.getClassLoader(), "", "info" );
      Eigenbase.setLogLevel( "info" );
      }
    else
      {
      setLogLevel( Main.class.getClassLoader(), "", "off" );
      Eigenbase.setLogLevel( "off" );
      EigenbaseTrace.getPlannerTracer().setLevel( Level.SEVERE );
      }
    }

  protected void printFailure( PrintStream errPrintStream, Throwable throwable )
    {
    errPrintStream.println( "command failed with: " + throwable.getMessage() );

    Throwable cause = Throwables.getRootCause( throwable );

    if( cause != null )
      {
      errPrintStream.println( "with cause: " + cause.getClass() );

      if( cause.getMessage() != null )
        errPrintStream.println( "          : " + cause.getMessage() );

      errPrintStream.println( Throwables.getStackTraceAsString( cause ) );
      }
    }
  }
