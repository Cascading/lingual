/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.lingual.optiq;

import java.util.HashMap;
import java.util.Map;

import cascading.lingual.optiq.meta.Ref;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.Splice;

/**
 * Contains state while tree of relational expressions is being traversed
 * to build a Cascading assembly.
 */
class Stack
  {
  Map<Class<? extends Splice>, Integer> spliceCounts = new HashMap<Class<? extends Splice>, Integer>();

  {
  spliceCounts.put( CoGroup.class, 0 );
  spliceCounts.put( GroupBy.class, 0 );
  spliceCounts.put( HashJoin.class, 0 );
  spliceCounts.put( Merge.class, 0 );
  }

  public final Map<Ref, Pipe> heads = new HashMap<Ref, Pipe>();

  private final DebugLevel debugLevel = DebugLevel.VERBOSE;

  public Stack()
    {
    }

  public Pipe addDebug( CascadingRelNode node, Pipe pipe )
    {
    if( pipe instanceof Each && ( (Each) pipe ).getOperation() instanceof Debug )
      return pipe;

    String name = makeName( node, pipe );

    return new Each( pipe, debugLevel, new Debug( name, true ) );
    }

  public Pipe addDebug( CascadingRelNode node, Pipe pipe, int index )
    {
    String name = makeName( node, pipe ) + "{" + index + "}";

    return new Each( pipe, debugLevel, new Debug( name, true ) );
    }

  public Pipe addDebug( CascadingRelNode node, Pipe pipe, String index )
    {
    String name = makeName( node, pipe ) + "{" + index + "}";

    return new Each( pipe, debugLevel, new Debug( name, true ) );
    }

  private String makeName( CascadingRelNode node, Pipe pipe )
    {
    String name = "";

    if( node != null )
      name = node.getClass().getSimpleName() + ":";

    name += pipe.getClass().getSimpleName() + "[" + pipe.getName() + "]";

    return name;
    }

  public String getNameFor( Class<? extends Splice> type, Pipe... pipes )
    {
    int count = spliceCounts.put( type, spliceCounts.get( type ) + 1 );

    StringBuilder buffer = new StringBuilder();

    buffer.append( "{" ).append( count ).append( "}" );

    for( int i = 0; i < pipes.length; i++ )
      {
      Pipe pipe = pipes[ i ];

      if( i != 0 )
        {
        if( type == GroupBy.class || type == Merge.class )
          buffer.append( "+" );
        else
          buffer.append( "*" ); // more semantically correct
        }

      buffer.append( pipe.getName() );
      }

    return buffer.toString();
    }
  }
