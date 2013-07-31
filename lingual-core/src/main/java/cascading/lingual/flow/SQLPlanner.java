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

package cascading.lingual.flow;

import java.util.Arrays;
import java.util.List;

import cascading.flow.AssemblyPlanner;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.lingual.optiq.enumerable.CascadingFlowRunnerEnumerable;
import cascading.pipe.Pipe;
import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;

/**
 * Class SQLPlanner is an implementation of an {@link AssemblyPlanner} that supports parsing ANSI SQL statements
 * against {@link cascading.tap.Tap} meta-data into a pipe assembly, which in turn is used by a
 * {@link cascading.flow.FlowConnector} to create a new {@link Flow} instance.
 * <p/>
 * SQLPlanner uses available meta-data from a partially initialized Flow instance created by the current
 * {@link cascading.flow.planner.FlowPlanner} for the given platform. Specifically, internal "table" meta-data
 * is created from source and sink Tap instances provided by the current {@link cascading.flow.FlowDef}.
 * <p/>
 * Currently table names as expected in the given SQL statement are expected to match the source and sink names, if
 * the SQL names have a schema, {@link #setDefaultSchema(String)} must be called, or it must be prepended to the
 * names as provided to the FlowDef instance.
 * <p/>
 * For example with this from clause {@code "select * from employees.salaries"}, either the source Tap should be
 * named {@code "employees.salaries"} or {@code sqlPlanner.setDefaultSchema( "employees" );} must be called if the source
 * Tap is named {@code "salaries"}. Both approaches result in an internal "table" being named
 * {@code "employees.salaries"}.
 *
 * @see FlowDef
 */
public class SQLPlanner implements AssemblyPlanner
  {
  protected String sql;
  protected String defaultSchema;
  protected String tailName;

  public SQLPlanner()
    {
    }

  public String getSql()
    {
    return sql;
    }

  public SQLPlanner setSql( String sql )
    {
    this.sql = sql;

    return this;
    }

  public String getTailName()
    {
    return tailName;
    }

  public SQLPlanner setTailName( String tailName )
    {
    this.tailName = tailName;

    return this;
    }

  public String getDefaultSchema()
    {
    return defaultSchema;
    }

  public SQLPlanner setDefaultSchema( String defaultSchema )
    {
    this.defaultSchema = defaultSchema;

    return this;
    }

  @Override
  public List<Pipe> resolveTails( Context context )
    {
    if( getSql() == null )
      throw new IllegalStateException( "a sql statement must be provided" );

    Flow flow = context.getFlow();
    LingualContext lingualContext = new LingualContext( this, flow );

    OptiqPrepareImpl prepare = new OptiqPrepareImpl();

    OptiqPrepare.PrepareResult<Object> prepareResult = prepare.prepareSql( lingualContext, getSql(), null, Object[].class, -1 );

    Pipe current = ( (CascadingFlowRunnerEnumerable) prepareResult.getExecutable() ).getBranch().current;

    String name;

    if( getTailName() != null )
      name = getTailName();
    else if( flow.getSinks().size() == 1 )
      name = (String) flow.getSinkNames().get( 0 );
    else
      throw new IllegalStateException( "too many sinks to choose from, found: " + flow.getSinks().size() + ", use setTailName to match tail pipe with sink Tap" );

    current = new Pipe( name, current ); // bind the tail to the sink name

    return Arrays.asList( current );
    }
  }
