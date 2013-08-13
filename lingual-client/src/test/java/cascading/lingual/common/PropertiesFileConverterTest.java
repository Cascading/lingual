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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import com.google.common.collect.Maps;

/***
 * Tests for {@link PropertiesFileConverter}.
 * 
 */
public class PropertiesFileConverterTest
  {

  @Test
  public void testValuePattern()
    {
    PropertiesFileConverter pfc = new PropertiesFileConverter();
    assertEquals( "filename", pfc.valuePattern() );
    }

  @Test
  public void testValueType()
    {
    PropertiesFileConverter pfc = new PropertiesFileConverter();
    assertNull( pfc.valueType() );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertWithNonExistentFile()
    {
    PropertiesFileConverter pfc = new PropertiesFileConverter();
    pfc.convert( "/does/not/exist" );
    }

  @Test
  public void testConvert() throws IOException
    {
    File propsFile = File.createTempFile( "lingual.test.properties", null );
    propsFile.deleteOnExit();
    
    Properties testProps = new Properties();
    
    testProps.setProperty( "test1", "some value" );
    testProps.setProperty( "test2", "some other value" );
    
    testProps.store( new FileOutputStream( propsFile ), "" );

    PropertiesFileConverter pfc = new PropertiesFileConverter();
    Map<String, String> result = pfc.convert( propsFile.getAbsolutePath() );
    
    Map<String, String> expected = Maps.newHashMap();
    expected.put( "test1", "some value" );
    expected.put( "test2", "some other value" );
    
    assertEquals(expected, result);
    }
  }
