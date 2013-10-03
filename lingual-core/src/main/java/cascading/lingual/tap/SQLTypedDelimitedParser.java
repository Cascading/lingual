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

package cascading.lingual.tap;

import cascading.lingual.type.SQLTypeResolver;
import cascading.scheme.util.DelimitedParser;

/**
 * Class SQLTypedDelimitedParser is an implementation of {@link DelimitedParser} that can resolve
 * text file header information into field names and SQL based type information.
 * <p/>
 * It uses the {@link SQLTypeResolver} class.
 */
public class SQLTypedDelimitedParser extends DelimitedParser
  {
  public SQLTypedDelimitedParser( String delimiter, String quote )
    {
    super( delimiter, quote, new SQLTypeResolver() );
    }

  public SQLTypedDelimitedParser( String delimiter, String quote, boolean strict, boolean safe )
    {
    super( delimiter, quote, null, strict, safe, new SQLTypeResolver() );
    }

  }
