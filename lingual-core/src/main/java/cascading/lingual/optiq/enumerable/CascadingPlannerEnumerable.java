/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import cascading.lingual.platform.PlannerPlatformBroker;
import cascading.lingual.platform.PlatformBroker;
import net.hydromatic.linq4j.Enumerator;

/**
 *
 */
public class CascadingPlannerEnumerable extends CascadingFlowRunnerEnumerable
  {
  public CascadingPlannerEnumerable( long index )
    {
    super( index );

    // this has to happen in the constructor and not in enumerator(), since optiq is not calling
    // enumerator() for simple queries.
    PlatformBroker platformBroker = getPlatformBroker();

    if( platformBroker instanceof PlannerPlatformBroker )
      ( (PlannerPlatformBroker) platformBroker ).setTail( getBranch().current );
    }

  @Override
  public Enumerator enumerator()
    {
    return new Enumerator()
    {

    @Override
    public Object current()
      {
      return new Object[]{};
      }

    @Override
    public boolean moveNext()
      {
      return false;
      }

    @Override
    public void reset()
      {

      }

    @Override
    public void close()
      {

      }
    };
    }
  }
