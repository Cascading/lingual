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

import java.lang.ref.WeakReference;

import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.lingual.jdbc.LingualConnection;
import cascading.lingual.platform.PlatformBroker;

/**
 *
 */
public class AddResultTableListener implements FlowListener
  {
  private final PlatformBroker platformBroker;
  private final WeakReference<LingualConnection> lingualConnection;

  public AddResultTableListener( PlatformBroker platformBroker, LingualConnection lingualConnection )
    {
    this.platformBroker = platformBroker;
    this.lingualConnection = new WeakReference<LingualConnection>( lingualConnection );
    }

  @Override
  public void onStarting( Flow flow )
    {
    }

  @Override
  public void onStopping( Flow flow )
    {
    }

  @Override
  public void onCompleted( Flow flow )
    {
    if( !flow.getStats().isSuccessful() )
      return;

    platformBroker.addResultToSchema( flow.getSink(), lingualConnection.get() );
    }

  @Override
  public boolean onThrowable( Flow flow, Throwable throwable )
    {
    return false;
    }
  }
