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

import cascading.lingual.platform.PlatformBroker;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

/**
 *
 */
public class CascadingDataContext implements DataContext
  {
  private Schema rootSchema;
  private JavaTypeFactory typeFactory;
  private PlatformBroker platformBroker;

  public CascadingDataContext( Schema rootSchema, JavaTypeFactory typeFactory, PlatformBroker platformBroker )
    {
    this.rootSchema = rootSchema;
    this.typeFactory = typeFactory;
    this.platformBroker = platformBroker;
    }

  @Override
  public Schema getRootSchema()
    {
    return rootSchema;
    }

  @Override
  public JavaTypeFactory getTypeFactory()
    {
    return typeFactory;
    }

  public PlatformBroker getPlatformBroker()
    {
    return platformBroker;
    }

  @Override
  public Object get( String name )
    {
    return null;
    }
  }