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

package cascading.lingual.coercible;

import java.sql.Date;

import cascading.lingual.type.SQLDateCoercibleType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DateCoercionTest
  {

  @Test
  public void testDateCoercion()
    {
    SQLDateCoercibleType coercibleType = new SQLDateCoercibleType();

    // 2013-10-30 00:00:00 CET == 2013-10-29 23:00:00 GMT
    int timestampAsInt = 16007;
    Date input = new Date( 1383087600000L );

    Object canonical = coercibleType.canonical( input );
    Date coerced = (Date) coercibleType.coerce( canonical, java.sql.Date.class );

    // multiple coercions should preserve date. Run it 24 times to catch any timezone issue.
    for( int i = 0; i < 24; i++ )
      {
      canonical = coercibleType.canonical( coerced );
      assertEquals( "Canonical value changed on iteration " + i, timestampAsInt, ( (Integer) canonical ).intValue() );

      coerced = (Date) coercibleType.coerce( canonical, java.sql.Date.class );
      assertEquals( "Day of month value changed on iteration " + i + " now: " + coerced.toString(), input.getDate(), coerced.getDate() );
      assertEquals( "Month value changed on iteration " + i + " now: " + coerced.toString(), input.getMonth(), coerced.getMonth() );
      assertEquals( "Year value changed on iteration " + i + " now: " + coerced.toString(), input.getYear(), coerced.getYear() );
      }
    }

  }