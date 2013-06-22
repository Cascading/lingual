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

package cascading.lingual.type;

import java.lang.reflect.Type;
import java.util.Date;

import cascading.CascadingException;
import cascading.util.Util;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util14.DateTimeUtil;
import org.eigenbase.util14.ZonelessDate;
import org.eigenbase.util14.ZonelessDatetime;

/**
 *
 */
@JsonTypeName("SQLDate")
public class SQLDateCoercibleType extends SQLDateTimeCoercibleType
  {
  public SQLDateCoercibleType()
    {
    super( SqlTypeName.DATE );
    }

  protected ZonelessDatetime parse( String value )
    {
    return ZonelessDate.parse( value );
    }

  @Override
  protected ZonelessDatetime createInstance()
    {
    return new ZonelessDate();
    }

  @Override
  public Class getCanonicalType()
    {
    return Integer.class;
    }

  @Override
  public Object canonical( Object value )
    {
    if( value == null )
      return null;

    Class from = value.getClass();

    if( from == String.class )
      return (int) ( parse( (String) value ).getDateValue() / MILLIS_PER_DAY );

    if( from == Date.class )
      return (int) ( ( (Date) value ).getTime() / MILLIS_PER_DAY ); // in UTC

    if( from == Integer.class || from == int.class )
      return value;

    if( from == Long.class || from == long.class )
      return ( (Long) value ).intValue();

    throw new CascadingException( "unknown type coercion requested from: " + Util.getTypeName( from ) );
    }

  @Override
  public Object coerce( Object value, Type to )
    {
    if( value == null )
      return null;

    Class from = value.getClass();

    if( from != Integer.class )
      throw new IllegalStateException( "was not normalized" );

    // no coercion, or already in canonical form
    if( to == Integer.class || to == int.class || to == Object.class )
      return value;

    if( to == Long.class )
      return ( (Integer) value ).intValue();

    ZonelessDatetime date = createInstance();

    date.setZonelessTime( ( (Integer) value ).longValue() * MILLIS_PER_DAY );

    if( to == String.class )
      return date.toString();

    if( to == java.sql.Date.class )
      return new java.sql.Date( date.getJdbcDate( DateTimeUtil.defaultZone ) );

    if( to == java.sql.Timestamp.class )
      return new java.sql.Timestamp( date.getJdbcTimestamp( DateTimeUtil.defaultZone ) );

    if( to == java.sql.Time.class )
      return new java.sql.Time( date.getJdbcTime( DateTimeUtil.defaultZone ) );

    throw new CascadingException( "unknown type coercion requested, from: " + Util.getTypeName( from ) + " to: " + Util.getTypeName( to ) );
    }

  }
