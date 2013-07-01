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

package cascading.lingual.platform.hadoop;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A classloader that can instantiate a class from a hadoop {@link org.apache.hadoop.fs.Path} without needing to
 * download a file and load it. Data is read as bytes from the {@link java.io.InputStream} and the class is instantiated
 * from that. Once the class has been loaded from Hadoop, it can be accessed as a regular class<p>
 * Data gets re-pulled from Hadoop each time a class as asked for which can be painfully slow.
 * <p/>
 * Example taken from http://diablohorn.wordpress.com/2010/11/05/java-in-memory-class-loading/
 */
public class HadoopBackedClassloader extends ClassLoader
  {

  private final Logger LOG = LoggerFactory.getLogger( getClass().getName() );

  public HadoopBackedClassloader( ClassLoader parentClassloader )
    {
    super( parentClassloader );
    }

  public HadoopBackedClassloader()
    {
    super( HadoopBackedClassloader.class.getClassLoader() );
    }

  protected byte[] getClassBytes( String fullyQualifiedClassname, FSDataInputStream dataInputStream ) throws IOException
    {
    byte[] bytes = null;
    JarInputStream inputStream = new JarInputStream( new BufferedInputStream( dataInputStream ) );
    JarEntry jarEntry;
    String name;

    while( ( jarEntry = inputStream.getNextJarEntry() ) != null )
      {
      name = jarEntry.getName();

      if( name.equalsIgnoreCase( fullyQualifiedClassname ) )
        {
        bytes = new byte[ (int) jarEntry.getSize() ];
        inputStream.read( bytes, 0, bytes.length );
        }
      }

    return bytes;
    }

  public void loadHadoopClass( String fullyQualifiedClassname, FSDataInputStream dataInputStream ) throws IOException
    {
    if( null != findLoadedClass( fullyQualifiedClassname ) )
      LOG.debug( "Class {} already loaded", fullyQualifiedClassname );

    byte[] bytes = getClassBytes( fullyQualifiedClassname, dataInputStream );
    defineClass( fullyQualifiedClassname, bytes, 0, bytes.length, null );
    }
  }
