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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.util.Arrays;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class FactoryProviderJarCLITest extends CLIPlatformTestCase
  {
  private static final Logger LOG = LoggerFactory.getLogger( FactoryProviderJarCLITest.class );

  class JavaSourceFromString extends SimpleJavaFileObject
    {
    final String code;

    JavaSourceFromString( String name, String code )
      {
      super( URI.create( "string:///" + name.replace( '.', '/' ) + JavaFileObject.Kind.SOURCE.extension ), JavaFileObject.Kind.SOURCE );
      this.code = code;
      }

    @Override
    public CharSequence getCharContent( boolean ignoreEncodingErrors )
      {
      return code;
      }
    }

  public FactoryProviderJarCLITest()
    {
    super( true );
    }

  private String compileFactory( String path )
    {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();

    StringWriter writer = new StringWriter();
    PrintWriter out = new PrintWriter( writer );
    out.println( "package lingual.test;" );
    out.println( "public class ProviderFactory extends cascading.lingual.catalog.TestProviderFactory" );
    out.println( "  {" );
    out.println( "  }" );
    out.close();

    String className = "lingual.test.ProviderFactory";
    JavaFileObject file = new JavaSourceFromString( className, writer.toString() );

    new File( path ).mkdirs();

    String[] compileOptions = new String[]{"-d", path, "-classpath", System.getProperty( "java.class.path" )};
    Iterable<String> compilationOptions = Arrays.asList( compileOptions );

    Iterable<? extends JavaFileObject> compilationUnits = Arrays.asList( file );
    JavaCompiler.CompilationTask task = compiler.getTask( null, null, diagnostics, compilationOptions, null, compilationUnits );

    boolean success = task.call();
    for( Diagnostic diagnostic : diagnostics.getDiagnostics() )
      {
      LOG.info( diagnostic.getCode() );
      LOG.info( String.valueOf( diagnostic.getKind() ) );
      LOG.info( String.valueOf( diagnostic.getPosition() ) );
      LOG.info( String.valueOf( diagnostic.getStartPosition() ) );
      LOG.info( String.valueOf( diagnostic.getEndPosition() ) );
      LOG.info( String.valueOf( diagnostic.getSource() ) );
      LOG.info( String.valueOf( diagnostic.getMessage( null ) ) );
      }

    assertTrue( "compile failed", success );

    return path + className.replace( '.', '/' ) + JavaFileObject.Kind.CLASS.extension;
    }

  @Test
  public void testProviderWithSQLLine() throws IOException
    {
    copyFromLocal( SIMPLE_PRODUCTS_TABLE );

    String classPath = compileFactory( getFactoryPath() );
    createProviderJar( TEST_PROPERTIES_FACTORY_LOCATION, classPath );

    initCatalog();

    catalog( "--provider", "--add", getProviderPath() );

    SchemaCatalog schemaCatalog = getSchemaCatalog();
    Format format = Format.getFormat( "tpsv" );
    ProviderDef providerDef = schemaCatalog.findProviderDefFor( null, format );
    assertNotNull( "provider not registered to format", providerDef );
    assertEquals( "lingual.test.ProviderFactory", providerDef.getFactoryClassName() );

    Protocol protocol = Protocol.getProtocol( getPlatformName().equals( "hadoop" ) ? "hdfs" : "file" );
    schemaCatalog = getSchemaCatalog();
    providerDef = schemaCatalog.findProviderDefFor( null, protocol );
    assertNotNull( "provider not registered to protocol", providerDef );

    catalog( "--schema", "example", "--add" );
    catalog( "--schema", "example", "--table", "products", "--add", SIMPLE_PRODUCTS_TABLE );

    boolean result = shell( "--sql", PROVIDER_SQL_SELECT_FILE, "--platform", getPlatformName() );

    assertTrue( "unable to run query", result );
    }
  }
