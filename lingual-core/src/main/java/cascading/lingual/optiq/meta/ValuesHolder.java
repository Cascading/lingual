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

package cascading.lingual.optiq.meta;

import java.util.List;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import org.eigenbase.rex.RexLiteral;

/**
 *
 */
public class ValuesHolder
  {
  public final Map<String, TupleEntryCollector> cache;
  public final FlowProcess flowProcess;
  public final Tap tap;
  public final List<List<RexLiteral>> values;

  public ValuesHolder( Map<String, TupleEntryCollector> cache, FlowProcess flowProcess, Tap tap, List<List<RexLiteral>> values )
    {
    this.cache = cache;
    this.flowProcess = flowProcess;
    this.tap = tap;
    this.values = values;
    }
  }
