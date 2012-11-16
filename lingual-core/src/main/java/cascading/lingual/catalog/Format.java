/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.lingual.catalog;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * $Author: $
 * $Date: $
 * $Revision: $
 */
public enum Format
  {
    CSV( "csv" ),
    TSV( "tsv" ),
    TCSV( "tcsv" ),
    TTSV( "ttsv" );

  public static Format findFormatFor( String path )
    {
    String extension = path.replaceAll( ".*\\.([^.]+)$", "$1" );

    if( path == null || path.isEmpty() )
      return null;

    for( Format format : values() )
      {
      if( format.extensions.contains( extension.toLowerCase() ) )
        return format;
      }

    return null;
    }

  Set<String> extensions = new HashSet<String>();

  private Format( String... extensions )
    {
    Collections.addAll( this.extensions, extensions );
    }
  }
