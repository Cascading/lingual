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

package cascading.lingual.type;

import java.lang.reflect.Type;
import java.util.Calendar;
import java.util.Date;

import cascading.CascadingException;
import cascading.util.Util;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util14.DateTimeUtil;
import org.eigenbase.util14.ZonelessDate;
import org.eigenbase.util14.ZonelessDatetime;

/**
 * Handles conversion to and from the DATA data type in SQL. Since the DATE type is defined as not having a
 * timezone and having only yyyy-mm-dd precision we truncate rather than convert to preserve the symmetry of conversion. The
 * dates "2013-03-05 09:04:57 GMT" and "2013-03-05 04:04:57 -0700" are considered identical DATE values even though they represent
 * different actual calender times. This is in line with the convention that many databases have of allowing an implicit conversion
 * of DATE to TIMESTAMP and setting it to "yyyy-mm-dd 00:00:00" of their set default timezone.
 * <p/>
 * Symmetry is not guaranteed across Lingual jobs running in JVMs set to different timezones. Set UTC time on all servers or use the
 * TIMESTAMP datatype and {@link SQLDateTimeCoercibleType} since those are defined to represent a more precise time.
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
    if( value != null && value.endsWith( "+00" ) )
      value = value.substring( 0, value.length() - 3 );

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
      {
      Calendar calendar = DateTimeUtil.parseDateFormat( (String) value, DateTimeUtil.DateFormatStr, DateTimeUtil.defaultZone );
      return truncateDate( calendar.getTimeInMillis() );
      }

    if( Date.class.isAssignableFrom( from ) )
      {
      Date fromDate = (Date) value;
      return truncateDate( fromDate.getTime() );
      }

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

    // offset by the current timezone so we get only yyyy-mm-hh precision per the spec for DATE
    // Even with a "cast" of DATE to TIMESTAMP the value should be considered "yyyy-mm-hh 00:00:00"
    Calendar calendar = Calendar.getInstance();
    int timezoneOffset = calendar.get( Calendar.ZONE_OFFSET ) + calendar.get( Calendar.DST_OFFSET );
    long shiftedTime = ( ( (Integer) value ).longValue() * MILLIS_PER_DAY ) + timezoneOffset;

    ZonelessDatetime date = createInstance();

    date.setZonedTime( shiftedTime, DateTimeUtil.defaultZone );

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

  /** Converts a long to an int that can be used to represent the DATE datatype. */
  protected int truncateDate( long timestamp )
    {
    // The precision for DATE in SQL is only yyyy-mm-hh so truncate before rounding.
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis( timestamp );
    calendar.set( Calendar.HOUR_OF_DAY, 0 );
    calendar.set( Calendar.MINUTE, 0 );
    calendar.set( Calendar.SECOND, 0 );
    calendar.set( Calendar.MILLISECOND, 0 );
    long calTime = calendar.getTimeInMillis();

    return (int) Math.ceil( (double) calTime / MILLIS_PER_DAY );
    }

  }
