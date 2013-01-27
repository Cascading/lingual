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
import java.util.ArrayList;
import java.util.List;

import cascading.lingual.optiq.meta.ValuesHolder;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import net.hydromatic.linq4j.AbstractEnumerable;
import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;
import org.eigenbase.rex.RexLiteral;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CascadingValueInsertEnumerable extends AbstractEnumerable implements Enumerable
  {
  private static final Logger LOG = LoggerFactory.getLogger( CascadingValueInsertEnumerable.class );

  static final List<ValuesHolder> HOLDERS = new ArrayList<ValuesHolder>();

  protected final ValuesHolder valuesHolder;

  public static synchronized int addHolder( ValuesHolder valuesHolder )
    {
    HOLDERS.add( valuesHolder );

    return HOLDERS.size() - 1;
    }

  public static synchronized ValuesHolder getHolder( int index )
    {
    return HOLDERS.get( index );
    }

  public CascadingValueInsertEnumerable( int x )
    {
    valuesHolder = getHolder( x );
    }

  @Override
  public Enumerator enumerator()
    {
    // todo: cache collectors and only close when batch is completed
    TupleEntryCollector collector;

    try
      {
      collector = valuesHolder.tap.openForWrite( valuesHolder.flowProcess );
      }
    catch( IOException exception )
      {
      LOG.error( "open for write failed", exception );

      throw new RuntimeException( "open for write failed", exception );
      }

    long rowCount = 0;

    for( List<RexLiteral> values : valuesHolder.values )
      {
      Tuple tuple = Tuple.size( values.size() );

      for( int i = 0; i < values.size(); i++ )
        tuple.set( i, values.get( i ) );

      collector.add( tuple );

      rowCount++;
      }

    collector.close();

    return new Linq4j().singletonEnumerable( rowCount ).enumerator();
    }
  }
