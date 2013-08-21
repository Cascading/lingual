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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import com.google.common.base.Joiner;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ProviderJarCLIPlatformTest extends CLIPlatformTestCase
  {
  private static final String MAVEN_LIKE_PATH = "repo/com/test/provider/test-provider/1.0.0/";
  private static final String JAR_NAME = MAVEN_LIKE_PATH + "test-provider-1.0.0.jar";
  private static final String POM = MAVEN_LIKE_PATH + "test-provider-1.0.0.pom";
  private static final String SPEC = "com.test.provider:test-provider:1.0.0";
  private static final Logger LOG = LoggerFactory.getLogger( ProviderJarCLIPlatformTest.class );
  private static final String DEFAULT_PROVIDER_FACTORY_NAME = "ProviderFactory";

  public ProviderJarCLIPlatformTest()
    {
    super( true );
    }

  private Collection<File> compileFactory( String path, String simpleClassName )
    {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();

    StringWriter writer = new StringWriter();
    PrintWriter out = new PrintWriter( writer );
    out.println( "package lingual.test;" );
    out.println( "import java.util.Properties;" );
    out.println( "import cascading.scheme.Scheme;" );
    out.println( "import cascading.tap.SinkMode;" );
    out.println( "import cascading.tap.Tap;" );
    out.println( "import cascading.tuple.Fields;" );
    out.println( "import cascading.tap.MultiSourceTap;" );
    out.println( "public class " + simpleClassName + " extends cascading.lingual.catalog.TestProviderFactory" );
    out.println( "  {" );
    out.println( "  public Tap createTap( String protocol, Scheme scheme, String identifier, SinkMode mode, Properties properties )" );
    out.println( "    {" );
    out.println( "    return new MultiSourceTap( super.createTap( protocol, scheme, identifier, mode, properties ) )" );
    out.println( "      {" );
    out.println( "      boolean nada = false;" );
    out.println( "      };" );
    out.println( "    }" );
    out.println( "  }" );
    out.close();

    String className = "lingual.test." + simpleClassName;
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

    return FileUtils.listFiles( new File( path ), null, true );
    }

  private Collection<File> compileFactory( String path )
    {
    return compileFactory( path, DEFAULT_PROVIDER_FACTORY_NAME );
    }

  @Test
  public void testValidateJarProviderCLI() throws IOException
    {
    Collection<File> classPath = compileFactory( getFactoryPath() );
    createProviderJar( TEST_PROPERTIES_FACTORY_LOCATION, classPath, getProviderPath( TEST_PROVIDER_JAR_NAME ) );

    initCatalog();
    catalog( "--schema", EXAMPLE_SCHEMA, "--add", getSchemaPath( EXAMPLE_SCHEMA ) );
    int initialSize = getSchemaCatalog().getProviderNames( EXAMPLE_SCHEMA ).size();

    // validate a jar provider
    catalog( "--schema", EXAMPLE_SCHEMA, "--validate", "--provider", "--add", getProviderPath( TEST_PROVIDER_JAR_NAME ) );

    // intentionally fail
    catalog( false, "--schema", EXAMPLE_SCHEMA, "--validate", "--provider", "--add", "build/resources/test/jar/not-found-provider.jar" );

    // confirm that validate doesn't add any providers
    int finalSize = getSchemaCatalog().getProviderNames( EXAMPLE_SCHEMA ).size();
    assertEquals( "provider list should not have changed size", initialSize, finalSize );
    }

  @Test
  public void testValidateSpecProviderCLI() throws IOException
    {
    makeTestMavenRepo();
    initCatalog();

    catalog( "--schema", EXAMPLE_SCHEMA, "--add", getSchemaPath( EXAMPLE_SCHEMA ) );
    int initialSize = getSchemaCatalog().getProviderNames( EXAMPLE_SCHEMA ).size();
    catalog( "--repo", "testRepo", "--add", new File( getProviderPath( "repo" ) ).getAbsolutePath() );

    // validate an actual spec
    catalog( "--schema", EXAMPLE_SCHEMA, "--validate", "--provider", "--add", SPEC );
    assertFalse( SPEC + " provider found in catalog: " + getSchemaCatalog().getProviderNames(), getSchemaCatalog().getProviderNames().contains( SPEC ) );

    // fail a bogus spec
    catalog( false, "--schema", EXAMPLE_SCHEMA, "--validate", "--provider", "--add", "foo:bar:1.0" );

    // confirm that validate doesn't add any providers
    int finalSize = getSchemaCatalog().getProviderNames( EXAMPLE_SCHEMA ).size();
    assertEquals( "provider list should not have changed size", initialSize, finalSize );
    }

  @Test
  public void testValidatePropertiesProviderCLI() throws IOException
    {
    initCatalog();

    catalog( "--schema", EXAMPLE_SCHEMA, "--add", getSchemaPath( EXAMPLE_SCHEMA ) );
    int initialSize = getSchemaCatalog().getProviderNames( EXAMPLE_SCHEMA ).size();

    // A provider defined entirely on the CLI always passes validation but should
    // still result in not adding the provider
    catalog( "--schema", EXAMPLE_SCHEMA,
      "--format", "psv", "--validate", "--extensions", ".tpsv", "--provider", "bar",
      "--properties", "delimiter=|,typed=true,quote='"
    );

    // A provider defined entirely on the CLI always passes validation.
    // But this call to --properties without an arg should still get a CLI error.
    catalog( false, "--schema", EXAMPLE_SCHEMA,
      "--format", "psv", "--validate", "--extensions", ".tpsv", "--provider", "text",
      "--properties"
    );

    // confirm that validate doesn't add any providers
    int finalSize = getSchemaCatalog().getProviderNames( EXAMPLE_SCHEMA ).size();
    assertEquals( "provider list should not have changed size", initialSize, finalSize );
    }

  @Test
  public void testRemoveProviderCLI() throws IOException
    {
    Collection<File> classPath = compileFactory( getFactoryPath() );
    createProviderJar( TEST_PROPERTIES_FACTORY_LOCATION, classPath, getProviderPath( TEST_PROVIDER_JAR_NAME ) );

    makeTestMavenRepo();
    initCatalog();
    catalog( "--schema", EXAMPLE_SCHEMA, "--add", getSchemaPath( EXAMPLE_SCHEMA ) );

    // spec-based
    catalog( "--repo", "testRepo", "--add", new File( getProviderPath( "repo" ) ).getAbsolutePath() );
    catalog( "--schema", EXAMPLE_SCHEMA, "--provider", "--add", SPEC );
    Collection<String> specProviderNames = getSchemaCatalog().getProviderNames( EXAMPLE_SCHEMA );
    assertTrue( "spec-based provider not added to catalog: " + specProviderNames.toString(), specProviderNames.contains( getSpecProviderName() ) );
    catalog( "--schema", EXAMPLE_SCHEMA, "--provider", getSpecProviderName(), "--remove" );
    specProviderNames = getSchemaCatalog().getProviderNames( EXAMPLE_SCHEMA );
    assertFalse( "spec-based provider not removed from catalog: " + specProviderNames.toString(), specProviderNames.contains( getSpecProviderName() ) );

    // jar-based
    catalog( "--schema", EXAMPLE_SCHEMA, "--provider", "--add", getProviderPath( TEST_PROVIDER_JAR_NAME ) );
    Collection<String> jarProviderNames = getSchemaCatalog().getProviderNames( EXAMPLE_SCHEMA );
    assertTrue( "jar-based provider not added to catalog: " + jarProviderNames.toString(), jarProviderNames.contains( getSpecProviderName() ) );
    catalog( "--schema", EXAMPLE_SCHEMA, "--provider", getSpecProviderName(), "--remove" );
    jarProviderNames = getSchemaCatalog().getProviderNames( EXAMPLE_SCHEMA );
    assertFalse( "jar-based provider not removed from catalog: " + jarProviderNames.toString(), jarProviderNames.contains( getSpecProviderName() ) );
    }

  @Test
  public void testUpdateProviderCLI() throws IOException
    {
    Collection<File> classPath = compileFactory( getFactoryPath() );
    String firstFileHash = createProviderJar( TEST_PROPERTIES_FACTORY_LOCATION, classPath, getProviderPath( TEST_PROVIDER_JAR_NAME ) );

    initCatalog();
    catalog( "--schema", EXAMPLE_SCHEMA, "--add", getSchemaPath( EXAMPLE_SCHEMA ) );

    catalog( "--schema", EXAMPLE_SCHEMA, "--provider", "--add", getProviderPath( TEST_PROVIDER_JAR_NAME ) );
    Collection<String> jarProviderNames = getSchemaCatalog().getProviderNames( EXAMPLE_SCHEMA );
    assertTrue( "first provider not added to catalog: " + jarProviderNames.toString(), jarProviderNames.contains( getSpecProviderName() ) );

    classPath = compileFactory( getFactoryPath(), DEFAULT_PROVIDER_FACTORY_NAME + "New" );
    String secondFileHash = createProviderJar( TEST_PROPERTIES_FACTORY_LOCATION, classPath, getProviderPath( TEST_PROVIDER_JAR_NAME ) );
    assertFalse( "second compiled provider showed same md5 sum as first:", secondFileHash.equals( firstFileHash ) );

    // since upload happens before catalog add, this would have throw an exception out if we didn't replace the jar. \
    // but check that the catalog got updated properly.
    catalog( "--schema", EXAMPLE_SCHEMA, "--provider", getSpecProviderName(), "--update", getProviderPath( TEST_PROVIDER_JAR_NAME ) );
    jarProviderNames = getSchemaCatalog().getProviderNames( EXAMPLE_SCHEMA );
    ProviderDef providerDef = getSchemaCatalog().getSchemaDef( EXAMPLE_SCHEMA ).getProviderDef( jarProviderNames.iterator().next() );
    assertEquals( "provider description not updated in catalog", secondFileHash, providerDef.getFileHash() );
    }

  @Test
  public void testRenameProviderCLI() throws IOException
    {
    makeTestMavenRepo();
    initCatalog();

    catalog( "--schema", EXAMPLE_SCHEMA, "--add", getSchemaPath( EXAMPLE_SCHEMA ) );
    catalog( "--repo", "testRepo", "--add", new File( getProviderPath( "repo" ) ).getAbsolutePath() );

    catalog( "--schema", EXAMPLE_SCHEMA, "--provider", "--add", SPEC );
    Collection<String> providerNames = getSchemaCatalog().getProviderNames( EXAMPLE_SCHEMA );
    assertTrue( "provider " + getSpecProviderName() + " not added to catalog: " + providerNames.toString(), providerNames.contains( getSpecProviderName() ) );

    catalog( "--schema", EXAMPLE_SCHEMA, "--provider", getSpecProviderName(), "--rename", "renamed-spec" );

    assertFalse( "provider " + getSpecProviderName() + " still in catalog: " + providerNames.toString(), providerNames.contains( getSpecProviderName() ) );
    assertTrue( "provider " + "renamed-spec" + " not in catalog: " + providerNames.toString(), providerNames.contains( "renamed-spec" ) );

    ProviderDef providerDef = getSchemaCatalog().findProviderFor( EXAMPLE_SCHEMA, "renamed-spec" );
    Map<String, String> providerProps = providerDef.getProperties();
    String keyName;
    if( getPlatform().getName().equals( "local" ) )
      keyName = "cascading.bind.provider.pipe-local.format.tpsv.delimiter";
    else
      keyName = "cascading.bind.provider.pipe-hadoop.format.tpsv.delimiter";
    assertTrue( "renamed provider did not retain properties in " + providerProps.toString(), providerProps.containsKey( keyName ) );
    assertEquals( "renamed provider did not retain value in " + providerProps.toString(), "|", providerProps.get( keyName ) );
    }

  @Test
  public void testProviderWithSQLLine() throws IOException
    {
    copyFromLocal( SIMPLE_PRODUCTS_TABLE );

    createProviderJar( TEST_PROPERTIES_EXTENDS_LOCATION, Collections.<File>emptyList(), getProviderPath( TEST_PROVIDER_JAR_NAME ) );

    initCatalog();

    catalog( "--provider", "--add", getProviderPath( TEST_PROVIDER_JAR_NAME ) );

    SchemaCatalog schemaCatalog = getSchemaCatalog();
    Format format = Format.getFormat( "tpsv" );
    ProviderDef providerDef = schemaCatalog.findProviderDefFor( null, format );
    assertNotNull( "provider not registered to format", providerDef );

    Protocol protocol = Protocol.getProtocol( getPlatformName().equals( "hadoop" ) ? "hdfs" : "file" );
    schemaCatalog = getSchemaCatalog();
    providerDef = schemaCatalog.findProviderDefFor( null, protocol );
    assertNotNull( "provider not registered to protocol", providerDef );

    catalog( "--schema", EXAMPLE_SCHEMA, "--add", getSchemaPath( EXAMPLE_SCHEMA ) );
    catalog( "--schema", EXAMPLE_SCHEMA, "--table", "products", "--add", SIMPLE_PRODUCTS_TABLE );

    shellSQL( "select * from \"example\".\"products\";" );
    }

  @Test
  public void testProviderPropertiesWithSQLLine() throws IOException
    {
    copyFromLocal( SIMPLE_PRODUCTS_TABLE );

    initCatalog();

    SchemaCatalog schemaCatalog = getSchemaCatalog();

    catalog( "--schema", EXAMPLE_SCHEMA, "--add", getSchemaPath( EXAMPLE_SCHEMA ) );

    catalog( "--schema", EXAMPLE_SCHEMA,
      "--format", "psv", "--add", "--extensions", ".tpsv", "--provider", "text",
      "--properties", "delimiter=|,typed=true,quote='"
    );

    catalog( "--schema", EXAMPLE_SCHEMA, "--table", "products", "--add", SIMPLE_PRODUCTS_TABLE );

    Format format = Format.getFormat( "psv" );
    ProviderDef providerDef = schemaCatalog.findProviderDefFor( "example", format );
    assertNotNull( "provider not registered to format", providerDef );

    catalog( "--provider", providerDef.getName(), "--show" );

    shellSQL( "select * from \"example\".\"products\";" );
    }

  @Test
  public void testJarProviderWithSQLLine() throws IOException
    {
    copyFromLocal( SIMPLE_PRODUCTS_TABLE );

    Collection<File> classPath = compileFactory( getFactoryPath() );
    createProviderJar( TEST_PROPERTIES_FACTORY_LOCATION, classPath, getProviderPath( TEST_PROVIDER_JAR_NAME ) );

    initCatalog();

    catalog( "--schema", EXAMPLE_SCHEMA, "--add", getSchemaPath( EXAMPLE_SCHEMA ) );
    catalog( "--schema", EXAMPLE_SCHEMA, "--provider", "--add", getProviderPath( TEST_PROVIDER_JAR_NAME ) );

    catalog( "--schema", "results", "--add", getSchemaPath( "results" ) );
    catalog(
      "--stereotype", "results", "--add",
      "--columns", Joiner.on( "," ).join( PRODUCTS_COLUMNS ),
      "--types", Joiner.on( "," ).join( PRODUCTS_COLUMN_TYPES )
    );
    catalog( "--schema", "results", "--table", "results", "--add", getTablePath(), "--stereotype", "results" );

    SchemaCatalog schemaCatalog = getSchemaCatalog();
    Format format = Format.getFormat( "tpsv" );
    ProviderDef providerDef = schemaCatalog.findProviderDefFor( EXAMPLE_SCHEMA, format );
    assertNotNull( "provider not registered to format", providerDef );
    assertEquals( "lingual.test.ProviderFactory", providerDef.getFactoryClassName() );

    Protocol protocol = Protocol.getProtocol( getPlatformName().equals( "hadoop" ) ? "hdfs" : "file" );
    schemaCatalog = getSchemaCatalog();
    providerDef = schemaCatalog.findProviderDefFor( null, protocol );
    assertNotNull( "provider not registered to protocol", providerDef );

    catalog( "--schema", EXAMPLE_SCHEMA, "--table", "products", "--add", SIMPLE_PRODUCTS_TABLE );

    // read a file
    shellSQL( "select * from \"example\".\"products\";" ) ;
    // spawn a job
    shellSQL( "select * from \"example\".\"products\" where SKU is not null;" ) ;
    // spawn results into a unique table/scheme with differing providers meta-data
    shellSQL( "insert into \"results\".\"results\" select * from \"example\".\"products\" where SKU is not null;" );
    }

  @Test
  public void testSpecProviderWithSQLLine() throws IOException
    {
    copyFromLocal( SIMPLE_PRODUCTS_TABLE );
    makeTestMavenRepo();
    initCatalog();

    catalog( "--schema", EXAMPLE_SCHEMA, "--add", getSchemaPath( EXAMPLE_SCHEMA ) );
    catalog( "--repo", "testRepo", "--add", new File( getProviderPath( "repo" ) ).getAbsolutePath() );
    catalog( "--schema", EXAMPLE_SCHEMA, "--provider", "--add", SPEC );

    catalog( "--schema", "results", "--add", getSchemaPath( "results" ) );
    catalog(
      "--stereotype", "results", "--add",
      "--columns", Joiner.on( "," ).join( PRODUCTS_COLUMNS ),
      "--types", Joiner.on( "," ).join( PRODUCTS_COLUMN_TYPES )
    );
    catalog( "--schema", "results", "--table", "results", "--add", getTablePath(), "--stereotype", "results" );

    SchemaCatalog schemaCatalog = getSchemaCatalog();
    Format format = Format.getFormat( "tpsv" );
    ProviderDef providerDef = schemaCatalog.findProviderDefFor( "example", format );
    assertNotNull( "provider not registered to format", providerDef );
    assertEquals( "lingual.test.ProviderFactory", providerDef.getFactoryClassName() );

    Protocol protocol = Protocol.getProtocol( getPlatformName().equals( "hadoop" ) ? "hdfs" : "file" );
    schemaCatalog = getSchemaCatalog();
    providerDef = schemaCatalog.findProviderDefFor( null, protocol );
    assertNotNull( "provider not registered to protocol", providerDef );

    catalog( "--schema", EXAMPLE_SCHEMA, "--table", "products", "--add", SIMPLE_PRODUCTS_TABLE );

    // read a file
    shellSQL( "select * from \"example\".\"products\";" );
    // spawn a job
    shellSQL( "select * from \"example\".\"products\" where SKU is not null;" );
    // spawn results into a unique table/scheme with differing providers meta-data
    shellSQL( "insert into \"results\".\"results\" select * from \"example\".\"products\" where SKU is not null;" );
    }

  protected void makeTestMavenRepo() throws IOException
    {
    Collection<File> classPath = compileFactory( getFactoryPath() );
    createProviderJar( TEST_PROPERTIES_FACTORY_LOCATION, classPath, getProviderPath( JAR_NAME ) );
    File pomFile = new File( getProviderPath( POM ) );

    pomFile.getParentFile().mkdirs();

    Files.copy( new File( TEST_PROPERTIES_POM ), pomFile );
    }

  protected String getSpecProviderName()
    {
    return "pipe-" + getPlatform().getName();
    }

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

  }
