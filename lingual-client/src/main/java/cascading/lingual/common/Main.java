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

package cascading.lingual.common;

import java.io.IOException;
import java.io.PrintStream;

/**
 *
 */
public abstract class Main<O extends Options>
  {
  protected final PrintStream outPrintStream;
  protected final PrintStream errPrintStream;

  private String[] args;

  protected O options;

  protected Main()
    {
    this.outPrintStream = System.out;
    this.errPrintStream = System.err;
    }

  protected Main( PrintStream outPrintStream, PrintStream errPrintStream )
    {
    this.outPrintStream = outPrintStream;
    this.errPrintStream = errPrintStream;
    }

  public O getOptions()
    {
    return options;
    }

  protected boolean printUsage()
    {
    if( !options.isHelp() )
      return false;

    options.printUsage( getErrPrintStream() );

    return true;
    }

  protected boolean printVersion()
    {
    if( !options.isVersion() )
      return false;

    options.printVersion( getErrPrintStream() );

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
  }
