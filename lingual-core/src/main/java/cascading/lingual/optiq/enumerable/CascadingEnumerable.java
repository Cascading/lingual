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

package cascading.lingual.optiq.enumerable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import cascading.flow.Flow;
import cascading.flow.planner.PlannerException;
import cascading.lingual.optiq.meta.Holder;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import com.google.common.base.Throwables;
import net.hydromatic.linq4j.AbstractEnumerable;
import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Enumerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CascadingEnumerable extends AbstractEnumerable implements Enumerable
  {
  private static final Logger LOG = LoggerFactory.getLogger( CascadingEnumerable.class );

  static final List<Holder> FLOWS = new ArrayList<Holder>();
  protected final Holder holder;

  public static synchronized int addHolder( Holder holder )
    {
    FLOWS.add( holder );

    return FLOWS.size() - 1;
    }

  public static synchronized Holder getHolder( int index )
    {
    return FLOWS.get( index );
    }

  public CascadingEnumerable( int x )
    {
    holder = getHolder( x );
    }

  @Override
  public Enumerator enumerator()
    {
    Flow flow;

    try
      {
      flow = holder.flowFactory.create();
      }
    catch( PlannerException exception )
      {
      LOG.error( "planner failed", exception );

      if( holder.dotPath != null )
        {
        LOG.info( "writing flow dot: {}", holder.dotPath );
        exception.writeDOT( holder.dotPath );
        }

      throw exception;
      }

    if( holder.dotPath != null )
      {
      LOG.info( "writing flow dot: {}", holder.dotPath );
      flow.writeDOT( holder.dotPath );
      }

    try
      {
      LOG.debug( "starting flow: {}", flow.getName() );
      flow.complete(); // need to block
      LOG.debug( "completed flow: {}", flow.getName() );
      }
    catch( Exception exception )
      {
      LOG.error( "flow failed", exception );

      Throwable rootCause = Throwables.getRootCause( exception );

      if( rootCause != null && exception != rootCause )
        LOG.error( "with root cause", rootCause );

      throw new RuntimeException( "flow failed", exception );
      }

    LOG.debug( "reading results fields: {}", flow.getSink().getSinkFields().printVerbose() );

    if( flow.getSink().getSinkFields().size() == 1 )
      return new FlowObjectEnumerator( flow );
    else
      return new FlowArrayEnumerator( flow );
    }

  static class FlowObjectEnumerator implements Enumerator<Object>
    {
    private final static Object DUMMY = new Object();

    Flow flow;
    Iterator<TupleEntry> iterator;
    Object current;

    public FlowObjectEnumerator( Flow flow )
      {
      this.flow = flow;
      iterator = openIterator( flow );
      current = DUMMY;
      }

    private TupleEntryIterator openIterator( Flow flow )
      {
      try
        {
        return flow.openSink();
        }
      catch( IOException e )
        {
        throw new RuntimeException( e );
        }
      }

    public Object current()
      {
      if( current == DUMMY )
        throw new NoSuchElementException();

      return current;
      }

    public boolean moveNext()
      {
      if( iterator.hasNext() )
        {
        current = toNextObject();
        return true;
        }

      current = DUMMY;

      return false;
      }

    private Object toNextObject()
      {
      return iterator.next().getObject( 0 );
      }

    public void reset()
      {
      iterator = openIterator( flow );
      current = DUMMY;
      }
    }

  static class FlowArrayEnumerator implements Enumerator<Object[]>
    {
    private final static Object[] DUMMY = new Object[ 0 ];

    Flow flow;
    Iterator<TupleEntry> iterator;
    Object[] current;

    public FlowArrayEnumerator( Flow flow )
      {
      this.flow = flow;
      iterator = openIterator( flow );
      current = DUMMY;
      }

    private TupleEntryIterator openIterator( Flow flow )
      {
      try
        {
        return flow.openSink();
        }
      catch( IOException e )
        {
        throw new RuntimeException( e );
        }
      }

    public Object[] current()
      {
      if( current == DUMMY )
        throw new NoSuchElementException();

      return current;
      }

    public boolean moveNext()
      {
      if( iterator.hasNext() )
        {
        current = toNextObjectArray();
        return true;
        }

      current = DUMMY;

      return false;
      }

    private Object[] toNextObjectArray()
      {
      TupleEntry entry = iterator.next();

      return Tuple.elements( entry.getTuple() ).toArray( new Object[ entry.size() ] );
      }

    public void reset()
      {
      iterator = openIterator( flow );
      current = DUMMY;
      }
    }
  }
