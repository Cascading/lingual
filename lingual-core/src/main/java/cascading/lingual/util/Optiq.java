/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import cascading.lingual.jdbc.Driver;
import org.eigenbase.relopt.volcano.VolcanoPlanner;

/**
 *
 */
public class Optiq
  {
  public static void writeSQLPlan( Properties properties, String name, VolcanoPlanner planner )
    {
    String path = getSQLPlanPath( properties, name );

    if( path == null )
      return;

    PrintWriter writer;

    try
      {
      File file = new File( path ).getAbsoluteFile();

      File parentDir = file.getParentFile();

      parentDir.mkdirs();
      writer = new PrintWriter( file );
      }
    catch( IOException exception )
      {
      throw new RuntimeException( "unable to write sql plan to: " + path, exception );
      }

    planner.dump( writer );

    writer.close();
    }

  private static String getSQLPlanPath( Properties properties, String name )
    {
    if( !properties.containsKey( Driver.SQL_PLAN_PATH_PROP ) )
      return null;

    String path = properties.getProperty( Driver.SQL_PLAN_PATH_PROP );

    if( !path.endsWith( "/" ) )
      path += "/";

    return path += name + ".txt";
    }
  }
