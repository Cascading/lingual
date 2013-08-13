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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import joptsimple.ValueConverter;

import com.google.common.collect.Maps;


/**
 * 
 * A {@link ValueConverter} implementation that reads a property file from a given path and return
 * a {@link Map} with all values in it.
 *
 */
public class PropertiesFileConverter implements ValueConverter<Map<String, String>>
  {

  @Override
  public Map<String, String> convert(String value)
    {
    Properties properties = new Properties();
    try
      {
      properties.load( new FileInputStream( new File(value) ) );
      
      Map<String,String> values = Maps.newHashMap();
      for (String key: properties.stringPropertyNames())
        values.put( key, properties.getProperty( key ) );
        
      return values;
      }
    catch ( IOException ioe )
      {
      throw new IllegalArgumentException("problem while reading properties file " + value, ioe);
      }
    }

  @Override
  public Class<Map<String, String>> valueType()
    {
    return null;
    }

  @Override
  public String valuePattern()
    {
    return "filename";
    }

  }
