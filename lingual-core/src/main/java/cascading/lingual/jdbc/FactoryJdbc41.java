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

package cascading.lingual.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Implementation of {@link Factory} for JDBC 4.1 (JDK 1.7).
 */
class FactoryJdbc41 implements Factory
  {
  public Connection createConnection(
    Connection connection, Properties connectionProperties) throws SQLException
    {
      return new LingualConnectionJdbc41(connection, connectionProperties);
    }

  static class LingualConnectionJdbc41 extends LingualConnection
    {
    protected LingualConnectionJdbc41(Connection parent, Properties properties) throws SQLException
      {
      super(parent, properties);
      }

    public String getSchema() throws SQLException
      {
      throw new UnsupportedOperationException();
      }

    public void abort(Executor executor) throws SQLException
      {
      throw new UnsupportedOperationException();
      }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException
      {
      throw new UnsupportedOperationException();
      }

    public int getNetworkTimeout() throws SQLException
      {
      throw new UnsupportedOperationException();
      }
    }
  }
