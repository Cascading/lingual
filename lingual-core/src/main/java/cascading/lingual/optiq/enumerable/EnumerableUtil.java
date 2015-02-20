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

import java.util.List;

import cascading.lingual.type.SQLDateCoercibleType;
import cascading.lingual.type.SQLTimeCoercibleType;
import cascading.tuple.Tuple;
import org.eigenbase.rex.RexLiteral;

/**
 *
 */
public class EnumerableUtil
  {
  public static final SQLDateCoercibleType SQL_DATE_COERCIBLE_TYPE = new SQLDateCoercibleType();
  public static final SQLTimeCoercibleType SQL_TIME_COERCIBLE_TYPE = new SQLTimeCoercibleType();

  static Tuple createTupleFrom( List<RexLiteral> values )
    {
    Tuple tuple = Tuple.size( values.size() );

    for( int i = 0; i < values.size(); i++ )
      {
      RexLiteral rexLiteral = values.get( i );

      Object value = null;
      String result;

      // this overcomes an inconsistency in getValue2 with regard to date and time being
      // canonically integers, but getValue2 returning long values.
      // should be resolved in a future optiq release.
      switch( rexLiteral.getType().getSqlTypeName() )
        {
        case DATE:
          result = rexLiteral.toString();

          if( !result.equals( "null" ) ) // workaround till first class support in optiq
            value = SQL_DATE_COERCIBLE_TYPE.canonical( result );

          break;
        case TIME:
          result = rexLiteral.toString();

          if( !result.equals( "null" ) ) // workaround till first class support in optiq
            value = SQL_TIME_COERCIBLE_TYPE.canonical( value );

          break;
        default:
          value = rexLiteral.getValue2();
          break;
        }

      tuple.set( i, value );
      }

    return tuple;
    }
  }
