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

package cascading.lingual.platform;


import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.lingual.jdbc.LingualConnection;

/**
 * Used to manage the {@link LingualConnection} list of what flows are running at a given time.
 * <p/>
 * The Lingual/Cascading notions for {@link Flow}, {@link cascading.lingual.jdbc.LingualStatement} and
 * {@link LingualConnection} do not map precisely to JDBC notions of the same hence this workaround.
 * <p/>
 * In JDBC there can be only one {@link java.sql.Statement} active for a {@link java.sql.Connection}
 * at a given instant. While Optiq and Lingual honor this in practice with one {@link Flow}
 * to {@link cascading.lingual.jdbc.LingualStatement} to {@link LingualConnection} the class structure allows
 * for a hypothetical case where {@link cascading.lingual.jdbc.LingualConnection#createStatement()} and similar methods
 * can be called multiple times and then each return value has {@link cascading.lingual.jdbc.LingualStatement#execute(String)}
 * called.
 * <p/>
 * Since Lingual defers {@link Flow} creation until statement execution, we can't know for
 * sure that there's only one Hadoop Flow tied to a LingualConnection. Rather than risk a case where we cancel the
 * wrong flow when {@link cascading.lingual.jdbc.LingualStatement#cancel()} is called this class keeps a
 * collection of all running {@link Flow} objects and only allows access to the "current" Flow when collection has only one is
 * running. {@link LingualConnectionFlowListener} is used to manage registering and unregistering a {@link Flow} when appropriate.
 */
public class LingualConnectionFlowListener implements FlowListener
  {
  private final LingualConnection lingualConnection;

  public LingualConnectionFlowListener( LingualConnection lingualConnection )
    {
    this.lingualConnection = lingualConnection;
    }

  @Override
  public void onStarting( Flow flow )
    {
    lingualConnection.trackFlow( flow );
    }

  @Override
  public void onStopping( Flow flow )
    {
    lingualConnection.unTrackFlow( flow );
    }

  @Override
  public void onCompleted( Flow flow )
    {
    lingualConnection.unTrackFlow( flow );
    }

  @Override
  public boolean onThrowable( Flow flow, Throwable throwable )
    {
    return false;
    }
  }
