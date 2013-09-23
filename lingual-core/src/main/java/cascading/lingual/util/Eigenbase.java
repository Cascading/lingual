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

package cascading.lingual.util;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eigenbase.util.EigenbaseException;

/**
 * Utility classes for manipulating eigenbase logging.
 * <p/>
 * Caching prevent logger from being gc'd and losing its setting.
 */
public class Eigenbase
  {
  static List<Logger> loggers = new ArrayList<Logger>();

  static
    {
    loggers.add( Logger.getLogger( EigenbaseException.class.getName() ) );
    }

  public static void setLogLevel( String level )
    {
    for( Logger logger : loggers )
      logger.setLevel( Level.parse( level.toUpperCase() ) );
    }
  }
