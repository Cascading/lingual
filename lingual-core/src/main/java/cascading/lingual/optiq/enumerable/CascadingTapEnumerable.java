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
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.TableDef;
import cascading.lingual.jdbc.Driver;
import cascading.lingual.optiq.meta.TableHolder;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.util.Misc;
import cascading.lingual.util.Optiq;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import net.hydromatic.linq4j.AbstractEnumerable;
import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.optiq.rules.java.PhysType;
import org.eigenbase.relopt.volcano.VolcanoPlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Enumerable that reads from a Cascading Tap. */
public class CascadingTapEnumerable extends AbstractEnumerable implements Enumerable
  {
  private static final Logger LOG = LoggerFactory.getLogger( CascadingTapEnumerable.class );

  static long holdersCount = 0;
  static final Map<Long, TableHolder> holders = new HashMap<Long, TableHolder>();

  protected final TableHolder tableHolder;

  public static synchronized long addHolder( TableHolder tableHolder )
    {
    long count = holdersCount++;

    holders.put( count, tableHolder );

    return count;
    }

  public static synchronized TableHolder popHolder( long index )
    {
    return holders.remove( index );
    }

  public CascadingTapEnumerable( long index )
    {
    tableHolder = popHolder( index );
    }

  public PhysType getPhysType()
    {
    return tableHolder.physType;
    }

  public TableDef getTableDef()
    {
    return tableHolder.tableDef;
    }

  public PlatformBroker getPlatformBroker()
    {
    return tableHolder.platformBroker;
    }

  public VolcanoPlanner getVolcanoPlanner()
    {
    return tableHolder.planner;
    }

  public Enumerator enumerator()
    {
    PlatformBroker platformBroker = getPlatformBroker();
    Properties properties = platformBroker.getProperties();

    Optiq.writeSQLPlan( properties, Misc.createUniqueName(), getVolcanoPlanner() );

    FlowProcess flowProcess = platformBroker.getFlowProcess();
    SchemaCatalog schemaCatalog = platformBroker.getCatalog();

    Tap tap = schemaCatalog.createTapFor( getTableDef(), SinkMode.KEEP );
    int size = tap.getSourceFields().size();

    Type[] types = new Type[ size ];

    for( int i = 0; i < size; i++ )
      types[ i ] = getPhysType().fieldClass( i );

    int maxRows = getMaxRows( properties );

    if( size == 1 )
      return new TapObjectEnumerator( maxRows, types, flowProcess, tap );
    else
      return new TapArrayEnumerator( maxRows, types, flowProcess, tap );
    }

  private int getMaxRows( Properties properties )
    {
    if( !properties.containsKey( Driver.MAX_ROWS ) )
      return Integer.MAX_VALUE;

    return Integer.parseInt( properties.getProperty( Driver.MAX_ROWS ) );
    }
  }
