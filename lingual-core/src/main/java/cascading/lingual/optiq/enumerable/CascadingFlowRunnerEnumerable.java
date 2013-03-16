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

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.flow.StepCounters;
import cascading.flow.planner.PlannerException;
import cascading.lingual.optiq.meta.FlowHolder;
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

  static long holdersCount = 0;
  static final Map<Long, FlowHolder> holders = new HashMap<Long, FlowHolder>();

  protected final FlowHolder flowHolder;

  public static synchronized long addHolder( FlowHolder flowHolder )
    {
    long count = holdersCount++;

    holders.put( count, flowHolder );

    return count;
    }

  public static synchronized FlowHolder popHolder( long index )
    {
    return holders.remove( index );
    }

  public CascadingFlowRunnerEnumerable( long index )
    {
    flowHolder = popHolder( index );
    }

  @Override
  public Enumerator enumerator()
    {
    // see https://issues.apache.org/jira/browse/HADOOP-7982
    Thread thread = Thread.currentThread();
    ClassLoader current = thread.getContextClassLoader();

    thread.setContextClassLoader( getClass().getClassLoader() );

    try
      {
      return createEnumerator();
      }
    finally
      {
      thread.setContextClassLoader( current );
      }
    }

  public Enumerator createEnumerator()
    {
    Flow flow;

    try
      {
      flow = flowHolder.flowFactory.create();
      }
    catch( PlannerException exception )
      {
      LOG.error( "planner failed", exception );

      if( flowHolder.flowPlanPath != null )
        {
        LOG.info( "writing flow dot: {}", flowHolder.flowPlanPath );
        exception.writeDOT( flowHolder.flowPlanPath );
        }

      throw exception;
      }

    if( flowHolder.flowPlanPath != null )
      {
      LOG.info( "writing flow dot: {}", flowHolder.flowPlanPath );
      flow.writeDOT( flowHolder.flowPlanPath );
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
      return new FlowObjectEnumerator( flowHolder.maxRows, types, flow );
    else
      return new FlowArrayEnumerator( flowHolder.maxRows, types, flow );
    }
  }
