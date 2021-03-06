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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import cascading.CascadingException;

/** Reflection is a reflection utility helper. */
public class Reflection
  {
  public static Object invokeStaticMethod( ClassLoader loader, String typeString, String methodName, Object[] parameters, Class[] parameterTypes )
    {
    Class type = loadClass( loader, typeString );

    return invokeStaticMethod( type, methodName, parameters, parameterTypes );
    }

  public static Class<?> loadClassSafe( String typeString )
    {
    try
      {
      return loadClass( typeString );
      }
    catch( Exception exception )
      {
      return null;
      }
    }

  public static Class<?> loadClass( String typeString )
    {
    return loadClass( Thread.currentThread().getContextClassLoader(), typeString );
    }

  public static Class<?> loadClass( ClassLoader loader, String typeString )
    {
    try
      {
      return loader.loadClass( typeString );
      }
    catch( ClassNotFoundException exception )
      {
      throw new CascadingException( "unable to load class: " + typeString, exception );
      }
    }

  public static Object newInstanceSafe( Class type )
    {
    if( type == null )
      return null;

    try
      {
      return newInstance( type );
      }
    catch( Exception exception )
      {
      return null;
      }
    }

  public static Object newInstance( Class type )
    {
    try
      {
      Constructor constructor = type.getDeclaredConstructor();

      constructor.setAccessible( true );

      return constructor.newInstance();
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to create new instance of: " + type.getName(), exception );
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

  public static <T> T getStaticField( Class type, String fieldName )
    {
    try
      {
      Field field = type.getDeclaredField( fieldName );

      field.setAccessible( true );

      return (T) field.get( null );
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to get static field: " + type.getName() + "." + fieldName, exception );
      }
    }

  public static <T> T invokeInstanceMethod( Object target, String methodName )
    {
    return invokeInstanceMethod( target, methodName, (Object[]) null, (Class[]) null );
    }

  public static <T> T invokeInstanceMethod( Object target, String methodName, Object parameter, Class parameterType )
    {
    return invokeInstanceMethod( target, methodName, new Object[]{parameter}, new Class[]{parameterType} );
    }

  public static <T> T invokeInstanceMethodSafe( Object target, String methodName, Object parameter, Class parameterType )
    {
    try
      {
      return invokeInstanceMethod( target, methodName, new Object[]{parameter}, new Class[]{parameterType} );
      }
    catch( Exception exception )
      {
      return null;
      }
    }

  public static <T> T invokeInstanceMethod( Object target, String methodName, Object[] parameters, Class[] parameterTypes )
    {
    try
      {
      Method method;

      try
        {
        method = target.getClass().getMethod( methodName, parameterTypes );
        }
      catch( NoSuchMethodException exception )
        {
        method = target.getClass().getDeclaredMethod( methodName, parameterTypes );
        }

      method.setAccessible( true );

      return (T) method.invoke( target, parameters );
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to invoke instance method: " + target.getClass().getName() + "." + methodName, exception );
      }
    }
  }
