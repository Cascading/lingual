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

package cascading.lingual.optiq;

import java.util.HashMap;
import java.util.Map;

import cascading.lingual.optiq.meta.Ref;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.pipe.Each;
import cascading.pipe.Pipe;

/**
 * Contains state while tree of relational expressions is being traversed
 * to build a Cascading assembly.
 */
public class Stack
  {
  public final Map<Ref, Pipe> heads = new HashMap<Ref, Pipe>();

  private final DebugLevel debugLevel = DebugLevel.VERBOSE;

  public Stack()
    {
    }

  public Each addDebug( CascadingRelNode node, Pipe pipe )
    {
    String name = makeName( node, pipe );

    return new Each( pipe, debugLevel, new Debug( name, true ) );
    }

  public Each addDebug( CascadingRelNode node, Pipe pipe, int index )
    {
    String name = makeName( node, pipe ) + "{" + index + "}";

    return new Each( pipe, debugLevel, new Debug( name, true ) );
    }

  public Each addDebug( CascadingRelNode node, Pipe pipe, String index )
    {
    String name = makeName( node, pipe ) + "{" + index + "}";

    return new Each( pipe, debugLevel, new Debug( name, true ) );
    }

  private String makeName( CascadingRelNode node, Pipe pipe )
    {
    String name = "";

    if( node != null )
      {
      name = node.getClass().getSimpleName() + ":";
      }

    name += pipe.getClass().getSimpleName() + "[" + pipe.getName() + "]";

    return name;
    }
  }
