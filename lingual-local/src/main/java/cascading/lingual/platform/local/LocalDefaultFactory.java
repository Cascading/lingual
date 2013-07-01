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

package cascading.lingual.platform.local;

import cascading.lingual.platform.provider.DefaultFactory;
import cascading.lingual.tap.local.SQLTypedTextDelimited;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

/**
 *
 */
public class LocalDefaultFactory extends DefaultFactory
  {
  @Override
  protected Tap newTap( Scheme scheme, String identifier, SinkMode mode )
    {
    return new FileTap( scheme, identifier, mode );
    }

  @Override
  protected Scheme newScheme( Fields fields, String delimiter, String quote, boolean typed )
    {
    if( typed )
      return new SQLTypedTextDelimited( fields, delimiter, quote );

    return new TextDelimited( fields, true, delimiter, quote );
    }
  }
