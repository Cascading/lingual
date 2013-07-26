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

import java.io.PrintStream;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;

/**
 *
 */
public class Printer
  {
  char[] borders = new char[]{'=', '-', '-'};

  private final PrintStream outPrintStream;

  public Printer( PrintStream outPrintStream )
    {
    this.outPrintStream = outPrintStream;
    }

  public PrintStream getOutPrintStream()
    {
    return outPrintStream;
    }

  public void printMap( String header, Map map )
    {
    printHeader( header, '=' );

    printMap( map, 0 );
    }

  private void printMap( Map map, int depth )
    {
    Set<Map.Entry> entries = map.entrySet();

    for( Map.Entry entry : entries )
      {
      String key = entry.getKey().toString();
      Object value = entry.getValue();

      if( value instanceof Map )
        {
        getOutPrintStream().println();

        printHeader( key, borders[ depth ] );

        printMap( (Map) value, depth + 1 );
        }
      else
        {
        if( value instanceof Collection )
          value = Joiner.on( "," ).skipNulls().join( (Collection) value );

        if( value != null )
          value = value.toString();

        getOutPrintStream().println( key + '=' + Strings.nullToEmpty( (String) value ) );
        }
      }
    }

  public void printFormatted( String formatString, Object... args )
    {
    getOutPrintStream().println( String.format( formatString, args ) );
    }

  public void printLines( String header, char border, Collection values )
    {
    printHeader( header, border );

    for( Object value : values )
      {
      if( value instanceof Collection )
        getOutPrintStream().println( Joiner.on( "," ).join( (Collection) value ) );
      else
        getOutPrintStream().println( value );
      }
    }

  protected void printHeader( String header, char border )
    {
    getOutPrintStream().println( header );

    getOutPrintStream().println( Strings.repeat( String.valueOf( border ), header.length() ) );
    }
  }
