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

package cascading.lingual.optiq.enumerable;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.collect.Iterators;
import net.hydromatic.linq4j.Enumerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
abstract class TapEnumerator<Result> implements Enumerator<Result>
  {
  protected final static Tuple DUMMY = new Tuple();

  private static final Logger LOG = LoggerFactory.getLogger( TapObjectEnumerator.class );
  final int maxRows;
  final Type[] types;
  final Tap tap;
  final FlowProcess flowProcess;
  Iterator<TupleEntry> iterator;
  Tuple current;

  protected TapEnumerator( int maxRows, Type[] types, FlowProcess flowProcess, Tap tap )
    {
    this.maxRows = maxRows; // defaults Integer.MAX_VALUE
    this.types = types;
    this.flowProcess = flowProcess;
    this.tap = tap;
    this.iterator = openIterator( this.flowProcess, this.tap );
    }

  protected Iterator<TupleEntry> openIterator( FlowProcess flowProcess, Tap tap )
    {
    try
      {
      if( maxRows != Integer.MAX_VALUE )
        {
        LOG.debug( "using connection properties maxRows: {}", maxRows );
        return Iterators.limit( tap.openForRead( flowProcess ), maxRows );
        }

      return tap.openForRead( flowProcess );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( exception );
      }
    }

  public abstract Result current();

  public boolean moveNext()
    {
    if( iterator.hasNext() )
      {
      current = toNextTuple();
      return true;
      }

    current = DUMMY;

    return false;
    }

  protected Tuple toNextTuple()
    {
    TupleEntry entry = iterator.next();

    return entry.getCoercedTuple( types );
    }

  public void reset()
    {
    iterator = openIterator( flowProcess, tap );
    current = DUMMY;
    }

  public void close()
    {
    // REVIEW:
    }
  }
