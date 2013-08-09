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

package cascading.lingual.common;

import java.util.Map;

import junit.framework.TestCase;

import static com.google.common.collect.Maps.difference;
import static com.google.common.collect.Maps.newHashMap;

/**
 *
 */
public class ConverterTest extends TestCase
  {
  public void testPropertiesConverter()
    {
    PropertiesConverter converter = new PropertiesConverter();

    Map<String, String> result;

    result = newHashMap();
    result.put( "foo", "bar" );
    result.put( "man", "chu" );
    assertTrue( difference( result, converter.convert( "foo=bar,man=chu" ) ).areEqual() );

    result = newHashMap();
    result.put( "delim", "" );
    assertTrue( difference( result, converter.convert( "delim=" ) ).areEqual() );

    result = newHashMap();
    result.put( "delim", "," );
    assertTrue( difference( result, converter.convert( "delim=," ) ).areEqual() );

    result = newHashMap();
    result.put( "delim", "," );
    result.put( "foo", "bar" );
    assertTrue( difference( result, converter.convert( "delim=,,foo=bar" ) ).areEqual() );

    result = newHashMap();
    result.put( "delim", "" );
    result.put( "foo", "bar" );
    assertTrue( difference( result, converter.convert( "delim=,foo=bar" ) ).areEqual() );

    result = newHashMap();
    result.put( "foo", "split string" );
    result.put( "bar", "42" );
    assertTrue( difference( result, converter.convert( "foo=split string,bar=42" ) ).areEqual() );
    }
  }
