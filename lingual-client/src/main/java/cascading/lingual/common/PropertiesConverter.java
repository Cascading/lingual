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

import joptsimple.ValueConverter;

import static com.google.common.base.Splitter.onPattern;

/**
 *
 */
public class PropertiesConverter implements ValueConverter<Map<String, String>>
  {
  @Override
  public Map<String, String> convert( String value )
    {
    // let foo=, foo => ,
    // let foo=,bar=1 foo => & bar => 1
    // let foo=bar1,bar2 foo => bar1,bar2 -- when used alone
    return onPattern( "(?<==),(?!,|$)|(?<!=),(?![,$])(?=[^=]+=)" ).withKeyValueSeparator( '=' ).split( value );
    }

  @Override
  public Class<Map<String, String>> valueType()
    {
    return null;
    }

  @Override
  public String valuePattern()
    {
    return "name=value,name=value2";
    }
  }
