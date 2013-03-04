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

import java.lang.reflect.Method;

import cascading.CascadingException;

/**
 *
 */
public class LogUtil
  {
  public static void setLogLevel( String level )
    {
    setLogLevel( LogUtil.class.getClassLoader(), "cascading", level );
    }

  public static void setLogLevel( Class type, String log, String level )
    {
    setLogLevel( type.getClassLoader(), log, level );
    }

  public static void setLogLevel( ClassLoader loader, String log, String level )
    {
    Object loggerObject = getLoggerObject( loader, log );

    Object levelObject = invokeStaticMethod( loader, "org.apache.log4j.Level", "toLevel",
      new Object[]{level}, new Class[]{String.class} );

    invokeInstanceMethod( loggerObject, "setLevel",
      new Object[]{levelObject}, new Class[]{levelObject.getClass()} );
    }

  public static String getLogLevel( ClassLoader loader, String log )
    {
    Object loggerObject = getLoggerObject( loader, log );

    Object level = invokeInstanceMethod( loggerObject, "getLevel", new Object[]{}, new Class[]{} );

    if( level == null )
      return "";

    return level.toString();
    }

  private static Object getLoggerObject( ClassLoader loader, String log )
    {
    if( log == null || log.isEmpty() )
      return invokeStaticMethod( loader, "org.apache.log4j.Logger", "getRootLogger", null, null );

    return invokeStaticMethod( loader, "org.apache.log4j.Logger", "getLogger",
      new Object[]{log}, new Class[]{String.class} );
    }

  public static Object invokeStaticMethod( ClassLoader loader, String typeString, String methodName, Object[] parameters, Class[] parameterTypes )
    {
    try
      {
      Class type = loader.loadClass( typeString );

      return invokeStaticMethod( type, methodName, parameters, parameterTypes );
      }
    catch( ClassNotFoundException exception )
      {
      throw new CascadingException( "unable to load class: " + typeString, exception );
      }
    }

  public static Object invokeStaticMethod( Class type, String methodName, Object[] parameters, Class[] parameterTypes )
    {
    try
      {
      Method method = type.getDeclaredMethod( methodName, parameterTypes );

      method.setAccessible( true );

      return method.invoke( null, parameters );
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to invoke static method: " + type.getName() + "." + methodName, exception );
      }
    }

  public static Object invokeInstanceMethod( Object target, String methodName, Object[] parameters, Class[] parameterTypes )
    {
    try
      {
      Method method = target.getClass().getMethod( methodName, parameterTypes );

      method.setAccessible( true );

      return method.invoke( target, parameters );
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to invoke instance method: " + target.getClass().getName() + "." + methodName, exception );
      }
    }
  }
