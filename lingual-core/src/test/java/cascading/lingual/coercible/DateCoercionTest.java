/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.lingual.coercible;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;

import cascading.lingual.type.SQLDateCoercibleType;
import cascading.lingual.type.SQLTimestampCoercibleType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DateCoercionTest
  {

  private static SQLDateCoercibleType sqlDateCoercibleType = new SQLDateCoercibleType();
  private static SQLTimestampCoercibleType sqlTimestampCoercibleType = new SQLTimestampCoercibleType();
  private static long timeAsLong = 1383087600000L;
  private static Date inputDate = new Date( timeAsLong );  // 2013-10-30 00:00:00 CET == 2013-10-29 23:00:00 GMT
  private static Timestamp inputTimestamp = new Timestamp( timeAsLong );  // 2013-10-30 00:00:00 CET == 2013-10-29 23:00:00 GMT


  @Test
  public void testFromCanonical()
    {
    Date coerced = (Date) sqlDateCoercibleType.coerce( getIntergerRepresentation().intValue(), java.sql.Date.class );

    assertEquals( "wrong year", inputDate.getYear(), coerced.getYear() );
    assertEquals( "wrong month", inputDate.getMonth(), coerced.getMonth() );
    assertEquals( "wrong day of month", inputDate.getDate(), coerced.getDate() );
    }

  @Test
  public void testToCanonical()
    {
    Integer canonical = (Integer) sqlDateCoercibleType.canonical( inputDate );

    assertEquals( "not converted to proper canonical form", getIntergerRepresentation(), canonical );
    }

  @Test
  public void testFromStringCoercion()
    {
    // as run in tests we go from UTC to local time by coercing.
    String dateAsString = "1996-08-03";

    Integer canonical = (Integer) sqlDateCoercibleType.canonical( dateAsString );
    Date coerced = (Date) sqlDateCoercibleType.coerce( canonical, java.sql.Date.class );
    assertEquals( "String parsing got wrong day", 3, coerced.getDate() );
    assertEquals( "String parsing got wrong month", 7, coerced.getMonth() );
    assertEquals( "String parsing got wrong year", 96, coerced.getYear() );;
    }

  @Test
  public void testTimestampFromStringCoercion()
    {
    // as run in tests we go from UTC to local time by coercing.
    String dateAsString = "1996-08-04 01:02:03";

    Long canonical = (Long) sqlTimestampCoercibleType.canonical( dateAsString );
    Timestamp coerced = (Timestamp) sqlTimestampCoercibleType.coerce( canonical, java.sql.Timestamp.class );
    assertEquals( "String parsing got wrong day", 4, coerced.getDate() );
    assertEquals( "String parsing got wrong month", 7, coerced.getMonth() );
    assertEquals( "String parsing got wrong year", 96, coerced.getYear() );
    assertEquals( "String parsing got wrong hour", 1, coerced.getHours() );
    assertEquals( "String parsing got wrong minute", 2, coerced.getMinutes());
    assertEquals( "String parsing got wrong second", 3, coerced.getSeconds() );
    }

  @Test
  public void testSymetricDateCoercion()
    {
    Object canonical = sqlDateCoercibleType.canonical( inputDate );
    Date coerced = (Date) sqlDateCoercibleType.coerce( canonical, java.sql.Date.class );

    // multiple coercions should preserve date. Run it 24 times to catch any timezone issue.
    for( int i = 0; i < 24; i++ )
      {
      canonical = sqlDateCoercibleType.canonical( coerced );
      assertEquals( "Canonical value changed on iteration " + i, getIntergerRepresentation().intValue(), ( (Integer) canonical ).intValue() );

      coerced = (Date) sqlDateCoercibleType.coerce( canonical, java.sql.Date.class );
      assertEquals( "Coerced day changed on iteration " + i + " now: " + coerced.toString(), inputDate.getDate(), coerced.getDate() );
      assertEquals( "Coerced month changed on iteration " + i + " now: " + coerced.toString(), inputDate.getMonth(), coerced.getMonth() );
      assertEquals( "Coerced year changed on iteration " + i + " now: " + coerced.toString(), inputDate.getYear(), coerced.getYear() );
      }
    }

  @Test
  public void testSymetricTimestampCoercion()
    {
    Object canonical = sqlTimestampCoercibleType.canonical( inputTimestamp );
    Timestamp coerced = (Timestamp) sqlTimestampCoercibleType.coerce( canonical, java.sql.Timestamp.class );

    // multiple coercions should preserve date. Run it 24 times to catch any timezone issue.
    for( int i = 0; i < 24; i++ )
      {
      canonical = sqlTimestampCoercibleType.canonical( coerced );
      assertEquals( "Canonical value changed on iteration " + i, timeAsLong, ( (Long) canonical ).longValue() );

      coerced = (Timestamp) sqlTimestampCoercibleType.coerce( canonical, java.sql.Timestamp.class );
      assertEquals( "Coerced day changed on iteration " + i + " now: " + coerced.toString(), inputTimestamp.getDate(), coerced.getDate() );
      assertEquals( "Coerced month changed on iteration " + i + " now: " + coerced.toString(), inputTimestamp.getMonth(), coerced.getMonth() );
      assertEquals( "Coerced year changed on iteration " + i + " now: " + coerced.toString(), inputTimestamp.getYear(), coerced.getYear() );
      assertEquals( "Coerced hour changed on iteration " + i + " now: " + coerced.toString(), inputTimestamp.getHours(), coerced.getHours() );
      }
    }

  protected Integer getIntergerRepresentation()
    {
    // depending on the rounding rules this may vary by timezone.
    // in general it's around 16007 or 16008.
    Calendar calendar = Calendar.getInstance();
    calendar.set( Calendar.YEAR, inputDate.getYear() + 1900 );
    calendar.set( Calendar.MONTH, inputDate.getMonth() );
    calendar.set( Calendar.DATE, inputDate.getDate() );
    calendar.set( Calendar.HOUR_OF_DAY, 0 );
    calendar.set( Calendar.MINUTE, 0 );
    calendar.set( Calendar.SECOND, 0 );
    calendar.set( Calendar.MILLISECOND, 0 );
    long calTime = calendar.getTimeInMillis();

    return (int) Math.ceil( (double) calTime / SQLDateCoercibleType.MILLIS_PER_DAY );
    }

  }