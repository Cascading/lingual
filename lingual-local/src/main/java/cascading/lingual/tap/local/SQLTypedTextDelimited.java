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

package cascading.lingual.tap.local;

import cascading.lingual.tap.SQLTypedDelimitedParser;
import cascading.scheme.local.TextDelimited;
import cascading.tuple.Fields;

/**
 * Class SQLTypedTextDelimited is an implementation of {@link TextDelimited} that
 * can resolve header information into field names and column types.
 * <p/>
 * It uses {@link cascading.lingual.type.SQLTypeResolver} to perform the mapping.
 */
public class SQLTypedTextDelimited extends TextDelimited
  {
  public SQLTypedTextDelimited( String delimiter, String quote )
    {
    super( new SQLTypedDelimitedParser( delimiter, quote ) );
    }

  public SQLTypedTextDelimited( Fields fields, String delimiter, String quote )
    {
    super( fields, true, new SQLTypedDelimitedParser( delimiter, quote ) );
    }
  }
