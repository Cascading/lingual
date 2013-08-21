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

package cascading.lingual.catalog;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class FormatProperties extends SchemaProperties
  {
  public static final String EXTENSIONS = "extensions";

  public static String findExtensionFor( SchemaDef schemaDef, Format format )
    {
    if( schemaDef == null )
      return null;

    List<String> extensions = schemaDef.getFormatProperty( format, EXTENSIONS );

    if( extensions.isEmpty() )
      return null;

    return extensions.get( 0 );
    }

  public static Format findFormatFor( SchemaDef schemaDef, String identifier )
    {
    if( schemaDef == null )
      return null;

    Map<Format, List<String>> extensions = schemaDef.findPropertyByFormats( EXTENSIONS );

    String extension = identifier.replaceAll( ".*\\.([^.]+)$", ".$1" );

    for( Map.Entry<Format, List<String>> entry : extensions.entrySet() )
      {
      if( entry.getValue().contains( extension ) )
        return entry.getKey();
      }

    return null;
    }
  }
