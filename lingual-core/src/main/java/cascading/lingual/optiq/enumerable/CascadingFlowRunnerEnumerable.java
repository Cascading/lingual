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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.flow.StepCounters;
import cascading.flow.planner.PlannerException;
import cascading.lingual.optiq.meta.FlowHolder;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import com.google.common.base.Throwables;
import net.hydromatic.linq4j.AbstractEnumerable;
import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CascadingFlowRunnerEnumerable extends AbstractEnumerable implements Enumerable
  {
  private static final Logger LOG = LoggerFactory.getLogger( CascadingFlowRunnerEnumerable.class );

  static final List<FlowHolder> HOLDERS = new ArrayList<FlowHolder>();

  protected final FlowHolder flowHolder;

  public static synchronized int addHolder( FlowHolder flowHolder )
    {
    HOLDERS.add( flowHolder );

    return HOLDERS.size() - 1;
    }

  public static synchronized FlowHolder getHolder( int index )
    {
    return HOLDERS.get( index );
    }

  public CascadingFlowRunnerEnumerable( int x )
    {
    flowHolder = getHolder( x );
    }

  @Override
  public Enumerator enumerator()
    {
    Flow flow;

    try
      {
      flow = flowHolder.flowFactory.create();
      }
    catch( PlannerException exception )
      {
      LOG.error( "planner failed", exception );

      if( flowHolder.dotPath != null )
        {
        LOG.info( "writing flow dot: {}", flowHolder.dotPath );
        exception.writeDOT( flowHolder.dotPath );
        }

      throw exception;
      }

    if( flowHolder.dotPath != null )
      {
      LOG.info( "writing flow dot: {}", flowHolder.dotPath );
      flow.writeDOT( flowHolder.dotPath );
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

    if( flowHolder.isModification )
      {
      FlowStep flowStep = (FlowStep) flow.getFlowSteps().get( flow.getFlowSteps().size() - 1 );
      long rowCount = flowStep.getFlowStepStats().getCounterValue( StepCounters.Tuples_Written );
      return new Linq4j().singletonEnumerable( rowCount ).enumerator();
      }

    int size = flow.getSink().getSinkFields().size();

    Type[] types = new Type[ size ];

    for( int i = 0; i < size; i++ )
      types[ i ] = flowHolder.physType.fieldClass( i );

    if( size == 1 )
      return new FlowObjectEnumerator( types, flow );
    else
      return new FlowArrayEnumerator( types, flow );
    }

  static class FlowObjectEnumerator implements Enumerator<Object>
    {
    private final static Object DUMMY = new Object();

    Type[] types;
    Flow flow;
    Iterator<TupleEntry> iterator;
    Object current;

    public FlowObjectEnumerator( Type[] types, Flow flow )
      {
      this.types = types;
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
      return iterator.next().getCoercedTuple( types ).getObject( 0 );
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

    Type[] types;
    Flow flow;
    Iterator<TupleEntry> iterator;
    Object[] current;

    public FlowArrayEnumerator( Type[] types, Flow flow )
      {
      this.types = types;
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

      Tuple result = entry.getCoercedTuple( types );

      return Tuple.elements( result ).toArray( new Object[ entry.size() ] );
      }

    public void reset()
      {
      iterator = openIterator( flow );
      current = DUMMY;
      }
    }
  }
