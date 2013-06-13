/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.lingual.platform;


import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.lingual.jdbc.LingualConnection;

/**
 *
 * Used to manage the {@link LingualConnection} list of what flows are running at a given time.
 * The Lingual/Cascading notions for {@link Flow}, {@link cascading.lingual.jdbc.LingualStatement} and
 * {@link LingualConnection} do not map precisely to JDBC notions of the same hence this workaround.
 * In JDBC there can be only one {@link java.sql.Statement} active for a {@link java.sql.Connection}
 * at a given instant. While Optiq and Lingual honor this in practice with one {@link Flow}
 * to {@link cascading.lingual.jdbc.LingualStatement} to {@link LingualConnection} the class structure allows
 * for a hypothetical case where {@link cascading.lingual.jdbc.LingualConnection#createStatement()} and similar methods
 * can be called multiple times and then each return value has {@link cascading.lingual.jdbc.LingualStatement#execute(String)}
 * called.<br></br>
 * Since Lingual defers {@link Flow} creation until statement execution, we can't know for
 * sure that there's only one Hadoop Flow tied to a LingualConnection. Rather than risk a case where we cancel the
 * wrong flow when {@link cascading.lingual.jdbc.LingualStatement#cancel()} is called this class keeps a
 * collection of all running {@link Flow} objects and only allows access to the "current" Flow when collection has only one is
 * running. {@link LingualConnectionFlowListener} is used to manage registering and unregistering a {@link Flow} when appropriate.
 */
public class LingualConnectionFlowListener implements FlowListener
  {

  private final LingualConnection lingualConnection;

  public LingualConnectionFlowListener( LingualConnection lingualConnection ) {
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
    lingualConnection.untrackFlow( flow );
    }

  @Override
  public void onCompleted( Flow flow )
    {
    lingualConnection.untrackFlow( flow );
    }

  @Override
  public boolean onThrowable( Flow flow, Throwable throwable )
    {
    return false;
    }
  }
