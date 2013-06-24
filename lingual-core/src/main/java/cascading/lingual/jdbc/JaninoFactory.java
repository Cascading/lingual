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

package cascading.lingual.jdbc;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

/** Implements {@link Factory} by generating a non-abstract derived class. */
class JaninoFactory implements Factory
  {
  public Connection createConnection( Connection connection, Properties connectionProperties ) throws SQLException
    {
    try
      {
      return create(
        LingualConnection.class,
        new Class[]{Connection.class, Properties.class},
        new Object[]{connection, connectionProperties}
      );
      }
    catch( Exception exception )
      {
      throw new SQLException( "could not create connection: " + exception.getMessage(), exception );
      }
    }

  static <T> T create( Class<T> abstractClass, Class[] constructorParamTypes, Object[] constructorArgs )
    throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, IOException, CompileException
    {
    Class targetType;

    if( !isAbstract( abstractClass.getModifiers() ) )
      {
      targetType = abstractClass;
      }
    else
      {
      final StringBuilder buf = new StringBuilder();

      buf.append( "public " )
        .append( "SC" )
        .append( "(" );

      for( int i = 0; i < constructorParamTypes.length; i++ )
        {
        Class constructorParamType = constructorParamTypes[ i ];

        if( i > 0 )
          buf.append( ", " );

        unparse( buf, constructorParamType )
          .append( " p" )
          .append( i );
        }

      buf.append( ") throws java.sql.SQLException { super(" );

      for( int i = 0; i < constructorParamTypes.length; i++ )
        {
        if( i > 0 )
          buf.append( ", " );

        buf.append( "p" )
          .append( i );
        }

      buf.append( "); }\n" );

      for( Method method : abstractClass.getMethods() )
        {
        if( isAbstract( method.getModifiers() ) )
          {
          buf.append( "public " );

          unparse( buf, method.getReturnType() )
            .append( " " )
            .append( method.getName() )
            .append( "(" );

          Class<?>[] parameterTypes = method.getParameterTypes();

          for( int i = 0; i < parameterTypes.length; i++ )
            {
            if( i > 0 )
              buf.append( ", " );

            Class<?> type = parameterTypes[ i ];

            unparse( buf, type ).append( " " ).append( "p" ).append( i );
            }

          buf.append( ") { throw new UnsupportedOperationException();}\n" );
          }
        }

      StringReader stringReader = new StringReader( buf.toString() );
      ClassLoader classLoader = JaninoFactory.class.getClassLoader();
      ClassBodyEvaluator evaluator = new ClassBodyEvaluator( new Scanner( null, stringReader ), abstractClass, new Class[ 0 ], classLoader );

      targetType = evaluator.getClazz();
      }

    Constructor constructor = targetType.getConstructor( constructorParamTypes );

    return abstractClass.cast( constructor.newInstance( constructorArgs ) );
    }

  private static <T> boolean isAbstract( int modifiers )
    {
    return ( modifiers & Modifier.ABSTRACT ) != 0;
    }

  private static StringBuilder unparse( StringBuilder buf, Class clazz )
    {
    if( clazz.isPrimitive() )
      return buf.append( clazz );
    else if( clazz.isArray() )
      return unparse( buf, clazz.getComponentType() ).append( "[]" );
    else
      return buf.append( clazz.getName() );
    }
  }
