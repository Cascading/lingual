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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import cascading.bind.catalog.Stereotype;
import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.flow.StepCounters;
import cascading.flow.planner.PlannerException;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.TableDef;
import cascading.lingual.jdbc.Driver;
import cascading.lingual.optiq.meta.Branch;
import cascading.lingual.optiq.meta.FlowHolder;
import cascading.lingual.optiq.meta.Ref;
import cascading.lingual.platform.LingualFlowFactory;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.util.Optiq;
import cascading.tap.SinkMode;
import cascading.tuple.TupleEntryCollector;
import com.google.common.base.Throwables;
import net.hydromatic.linq4j.AbstractEnumerable;
import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;
import org.eigenbase.relopt.volcano.VolcanoPlanner;
import org.eigenbase.rex.RexLiteral;
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

  public Branch getBranch()
    {
    return flowHolder.branch;
    }

  public PlatformBroker getPlatformBroker()
    {
    return flowHolder.branch.platformBroker;
    }

  public VolcanoPlanner getVolcanoPlanner()
    {
    return flowHolder.planner;
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
    catch( IOException exception )
      {
      throw new RuntimeException( "failed opening tap", exception );
      }
    finally
      {
      thread.setContextClassLoader( current );
      }
    }

  public Enumerator createEnumerator() throws IOException
    {
    PlatformBroker platformBroker = getPlatformBroker();
    Branch branch = getBranch();

    Properties properties = platformBroker.getProperties();

    for( Ref head : branch.heads.keySet() )
      {
      if( head.tuples != null )
        writeValuesTuple( platformBroker, head );
      }

    LingualFlowFactory flowFactory = platformBroker.getFlowFactory( branch );

    Optiq.writeSQLPlan( properties, flowFactory.getName(), getVolcanoPlanner() );

    for( Ref head : branch.heads.keySet() )
      flowFactory.addSource( head.name, getIdentifierFor( platformBroker, head ) );

    if( branch.resultName != null )
      {
      TableDef tableDef = platformBroker.getCatalog().resolveTableDef( branch.resultName );
      flowFactory.addSink( tableDef.getName(), tableDef.getIdentifier() );
      }
    else
      {
      flowFactory.addSink( branch.current.getName(), platformBroker.getResultPath( flowFactory.getName() ) );
      }

    String flowPlanPath = setFlowPlanPath( properties, flowFactory.getName() );

    Flow flow;

    try
      {
      flow = flowFactory.create();
      }
    catch( PlannerException exception )
      {
      LOG.error( "planner failed", exception );

      if( flowPlanPath != null )
        {
        LOG.info( "writing flow dot: {}", flowPlanPath );
        exception.writeDOT( flowPlanPath );
        }

      throw exception;
      }

    if( flowPlanPath != null )
      {
      LOG.info( "writing flow dot: {}", flowPlanPath );
      flow.writeDOT( flowPlanPath );
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

    if( branch.isModification )
      {
      FlowStep flowStep = (FlowStep) flow.getFlowSteps().get( flow.getFlowSteps().size() - 1 );
      long rowCount = flowStep.getFlowStepStats().getCounterValue( StepCounters.Tuples_Written );
      return new Linq4j().singletonEnumerable( rowCount ).enumerator();
      }

    int size = flow.getSink().getSinkFields().size();

    Type[] types = new Type[ size ];

    for( int i = 0; i < size; i++ )
      types[ i ] = flowHolder.physType.fieldClass( i );

    int maxRows = getMaxRows( properties );

    if( size == 1 )
      return new TapObjectEnumerator( maxRows, types, flow.getFlowProcess(), flow.getSink() );
    else
      return new TapArrayEnumerator( maxRows, types, flow.getFlowProcess(), flow.getSink() );
    }

  private String getIdentifierFor( PlatformBroker platformBroker, Ref head )
    {
    String identifier = head.identifier;

    if( identifier == null )
      identifier = platformBroker.getTempPath( head.name );

    return identifier;
    }

  private void writeValuesTuple( PlatformBroker platformBroker, Ref head ) throws IOException
    {
    SchemaCatalog catalog = platformBroker.getCatalog();
    String identifier = getIdentifierFor( platformBroker, head );

    createTableFor( catalog, head, identifier );

    TupleEntryCollector collector = catalog.createTapFor( identifier, SinkMode.KEEP ).openForWrite( platformBroker.getFlowProcess() );

    for( List<RexLiteral> values : head.tuples )
      collector.add( EnumerableUtil.createTupleFrom( values ) );

    collector.close();
    }

  private void createTableFor( SchemaCatalog catalog, Ref head, String identifier )
    {
    String stereotypeName = head.name;
    Stereotype stereotype = catalog.getStereoTypeFor( null, head.fields );

    if( stereotype != null )
      stereotypeName = stereotype.getName();
    else
      catalog.createStereotype( null, stereotypeName, head.fields );

    Protocol protocol = catalog.getRootSchemaDef().getDefaultProtocol();
    Format format = catalog.getRootSchemaDef().getDefaultFormat();

    catalog.createTableDefFor( null, head.name, identifier, stereotypeName, protocol, format );
    }

  private String setFlowPlanPath( Properties properties, String name )
    {
    if( !properties.containsKey( Driver.FLOW_PLAN_PATH ) )
      return null;

    String flowPlanPath = properties.getProperty( Driver.FLOW_PLAN_PATH );

    if( !flowPlanPath.endsWith( "/" ) )
      flowPlanPath += "/";

    flowPlanPath += name + ".dot";

    return flowPlanPath;
    }

  private int getMaxRows( Properties properties )
    {
    if( !properties.containsKey( Driver.MAX_ROWS ) )
      return Integer.MAX_VALUE;

    return Integer.parseInt( properties.getProperty( Driver.MAX_ROWS ) );
    }
  }
