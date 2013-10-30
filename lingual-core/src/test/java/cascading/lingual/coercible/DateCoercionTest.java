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

import static org.junit.Assert.*;

import java.sql.Date;

import org.junit.Test;

import cascading.lingual.type.SQLDateCoercibleType;
import cascading.lingual.type.SQLTimeCoercibleType;

public class DateCoercionTest
  {

  @Test
  public void testDateCoercion()
    {
    SQLDateCoercibleType sdct = new SQLDateCoercibleType();
    Date input = new Date( 1383087600000L ); // 2013-10-30 00:00:00 CET == 2013-10-29 23:00:00 GMT 

    Object canonical = sdct.canonical( input );
    Object reconverted = sdct.coerce( canonical, java.sql.Date.class );
    
    // due to a rounding bug the date would shift in each iteration.
    int i = 0;
    while( i < 10 )
      {
      i++;
      canonical = sdct.canonical( reconverted );
      reconverted = sdct.coerce( canonical, java.sql.Date.class );
      }
    assertEquals(input.getDay(), ((Date)reconverted).getDay());
    assertEquals(input.getMonth(), ((Date)reconverted).getMonth());
    assertEquals(input.getYear(), ((Date)reconverted).getYear());
    }
  }