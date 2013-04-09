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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.TableDef;
import cascading.lingual.optiq.CascadingEnumerableRel;
import cascading.lingual.optiq.meta.Branch;
import cascading.lingual.optiq.meta.ValuesHolder;
import cascading.lingual.platform.PlatformBroker;
import cascading.tap.SinkMode;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
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
public class CascadingValueInsertEnumerable extends AbstractEnumerable implements Enumerable
  {
  private static final Logger LOG = LoggerFactory.getLogger( CascadingValueInsertEnumerable.class );

  static long holdersCount = 0;
  static final Map<Long, ValuesHolder> holders = new HashMap<Long, ValuesHolder>();

  protected final ValuesHolder valuesHolder;

  public static synchronized long addHolder( ValuesHolder flowHolder )
    {
    long count = holdersCount++;

    holders.put( count, flowHolder );

    return count;
    }

  public static synchronized ValuesHolder popHolder( long index )
    {
    return holders.remove( index );
    }

  public CascadingValueInsertEnumerable( long index )
    {
    valuesHolder = popHolder( index );
    }

  public Branch getBranch()
    {
    return valuesHolder.branch;
    }

  public PlatformBroker getPlatformBroker()
    {
    return valuesHolder.branch.platformBroker;
    }

  public VolcanoPlanner getVolcanoPlanner()
    {
    return valuesHolder.planner;
    }

  @Override
  public Enumerator enumerator()
    {
    PlatformBroker platformBroker = getPlatformBroker();
    Branch branch = getBranch();

    CascadingEnumerableRel.writeSQLPlan( platformBroker.getProperties(), platformBroker.createUniqueName(), getVolcanoPlanner() );

    FlowProcess flowProcess = platformBroker.getFlowProcess();
    SchemaCatalog schemaCatalog = platformBroker.getCatalog();
    Map<String, TupleEntryCollector> cache = platformBroker.getCollectorCache();

    TableDef tableDef = platformBroker.getCatalog().resolveTableDef( branch.resultName );
    String identifier = tableDef.getIdentifier();

    TupleEntryCollector collector;

    try
      {
      if( cache != null && cache.containsKey( identifier ) )
        {
        LOG.debug( "inserting into (cached): {}", identifier );
        collector = cache.get( identifier );
        }
      else
        {
        LOG.debug( "inserting into: {}", identifier );
        collector = schemaCatalog.createTapFor( identifier, SinkMode.KEEP ).openForWrite( flowProcess );
        }

      if( cache != null )
        cache.put( identifier, collector );
      }
    catch( IOException exception )
      {
      LOG.error( "open for write failed", exception );

      throw new RuntimeException( "open for write failed", exception );
      }

    List<List<RexLiteral>> tuples = branch.tuples;
    long rowCount = 0;

    for( List<RexLiteral> values : tuples )
      {
      Tuple tuple = Tuple.size( values.size() );

      for( int i = 0; i < values.size(); i++ )
        tuple.set( i, values.get( i ).getValue2() ); // seem to come out canonical, so bypassing using TupleEntry to set

      collector.add( tuple );

      rowCount++;
      }

    LOG.debug( "inserted {} rows", rowCount );

    return new Linq4j().singletonEnumerable( rowCount ).enumerator();
    }
  }
