/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

/**
 *
 */
public class Head
  {
  public final String name;
  public final String identifier;

  public Head( String name, String identifier )
    {
    this.name = name;
    this.identifier = identifier;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Head head = (Head) object;

    if( identifier != null ? !identifier.equals( head.identifier ) : head.identifier != null )
      return false;
    if( name != null ? !name.equals( head.name ) : head.name != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + ( identifier != null ? identifier.hashCode() : 0 );
    return result;
    }
  }
