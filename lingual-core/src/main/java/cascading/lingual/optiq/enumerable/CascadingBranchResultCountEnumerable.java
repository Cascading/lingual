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
import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.lingual.optiq.meta.Branch;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;

/**
 * Placeholder enumerator for Flows that modify the contents and don't return enumerable data. The reference to
 * {@link Branch} and the {@link FlowProcess} are needed by later classes to bind results.
 */
public class CascadingBranchResultCountEnumerable extends TapEnumerator<Object>
  {
  private Enumerator<Long> resultEnumerator;
  private long resultCount;

  public CascadingBranchResultCountEnumerable( FlowProcess flowProcess, Tap tap, Branch branch, long resultCount )
    {
    super( 1, new Type[]{Long.class}, flowProcess, tap, branch );
    this.resultCount = resultCount;
    resultEnumerator = Linq4j.singletonEnumerable( resultCount ).enumerator();
    }

  @Override
  protected Iterator<TupleEntry> openIterator( FlowProcess flowProcess, Tap tap )
    {
    return null;
    }

  @Override
  public boolean moveNext()
    {
    return resultEnumerator.moveNext();
    }

  @Override
  protected Tuple toNextTuple()
    {
    resultEnumerator.moveNext();
    return new Tuple( resultCount );
    }

  @Override
  public Object current()
    {
    return resultEnumerator.current();
    }
  }
