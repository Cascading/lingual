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

import cascading.tuple.Fields;
import org.eigenbase.rex.RexLiteral;

/**
 *
 */
public class Ref
  {
  public final String name;
  public final String identifier;
  public final Fields fields;
  public final List<List<RexLiteral>> tuples;

  public Ref( String name, String identifier )
    {
    this.name = name;
    this.identifier = identifier;
    this.tuples = null;
    this.fields = null;
    }

  public Ref( String name, Fields fields, List<List<RexLiteral>> tuples )
    {
    this.name = name;
    this.identifier = null;
    this.fields = fields;
    this.tuples = tuples;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Ref ref = (Ref) object;

    if( fields != null ? !fields.equals( ref.fields ) : ref.fields != null )
      return false;
    if( identifier != null ? !identifier.equals( ref.identifier ) : ref.identifier != null )
      return false;
    if( name != null ? !name.equals( ref.name ) : ref.name != null )
      return false;
    if( tuples != null ? !tuples.equals( ref.tuples ) : ref.tuples != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + ( identifier != null ? identifier.hashCode() : 0 );
    result = 31 * result + ( fields != null ? fields.hashCode() : 0 );
    result = 31 * result + ( tuples != null ? tuples.hashCode() : 0 );
    return result;
    }
  }
