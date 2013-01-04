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

package cascading.lingual.catalog.ddl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cascading.bind.catalog.Stereotype;
import cascading.lingual.catalog.Format;
import cascading.lingual.catalog.Protocol;
import cascading.lingual.catalog.SchemaCatalog;
import cascading.lingual.catalog.SchemaDef;
import cascading.tuple.Fields;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import net.hydromatic.optiq.runtime.ByteString;
import org.eigenbase.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class DDLParser
  {
  private static final Logger LOG = LoggerFactory.getLogger( DDLParser.class );

  private final SchemaCatalog catalog;
  private final String schemaName;
  private final String schemaPath;
  private final Protocol protocol;
  private final Format format;

  public DDLParser( SchemaCatalog catalog, String schemaName, String protocol, String format )
    {
    this( catalog, schemaName, null, protocol, format );
    }

  public DDLParser( SchemaCatalog catalog, String schemaName, String schemaPath, String protocol, String format )
    {
    this.catalog = catalog;
    this.schemaName = schemaName;
    this.schemaPath = schemaPath == null ? getSchemaIdentifier( catalog, schemaName ) : schemaPath;
    this.protocol = Protocol.getProtocol( protocol );
    this.format = Format.getFormat( format );

    if( this.schemaPath == null )
      throw new IllegalArgumentException( "schemaPath must not be null" );
    }

  private String getSchemaIdentifier( SchemaCatalog catalog, String schemaName )
    {
    SchemaDef schemaDef = catalog.getSchemaDef( schemaName );

    if( schemaDef == null )
      return null;

    return schemaDef.getIdentifier();
    }

  public void apply( File file ) throws IOException
    {
    execute( parse( file ) );
    }

  public void execute( List<Table> commands )
    {
    SchemaDef schemaDef = catalog.getSchemaDef( schemaName );

    if( schemaDef == null )
      catalog.createSchemaDef( schemaName, schemaPath );
    else if( !schemaPath.equals( schemaDef.getIdentifier() ) )
      throw new IllegalStateException( "schema already exists with identifier: " + schemaPath );

    for( Table command : commands )
      {
      String name = command.name;
      Column[] columns = command.columns;

      switch( command.action )
        {
        case DROP:
          catalog.removeTableDef( schemaName, name );
          catalog.removeStereotype( schemaName, name );
          break;
        case CREATE:
          String stereotypeName = name;
          Fields fields = toFields( columns );
          Stereotype stereotype = catalog.getStereoTypeFor( schemaName, fields );

          if( stereotype != null )
            stereotypeName = stereotype.getName();
          else
            catalog.createStereotype( schemaName, stereotypeName, fields );

          catalog.createTableDefFor( schemaName, name, createTableIdentifier( name ), stereotypeName, protocol, format );
          break;
        }
      }
    }

  private String createTableIdentifier( String name )
    {
    return getSchemaIdentifier( catalog, schemaName ) + "/" + name;
    }

  public List<Table> parse( InputStream inputStream ) throws IOException
    {
    InputStreamReader stream = new InputStreamReader( inputStream );

    return parse( CharStreams.toString( stream ) );
    }

  public List<Table> parse( File file ) throws IOException
    {
    return parse( Files.toString( file, Charset.forName( "UTF-8" ) ) );
    }

  public List<Table> parse( String string )
    {
    Iterator<String> statements = parseStatements( string );

    List<Table> commands = new ArrayList<Table>();

    Pattern pattern = Pattern.compile( "^([^\\s]*)\\s+([^\\s]*)\\s+\"([^\\s]*)\"(.*)$" );

    while( statements.hasNext() )
      {
      String statement = statements.next();

      if( statement == null || statement.isEmpty() )
        continue;

      LOG.debug( "statement: {}", statement );

      String[] command = new String[ 4 ];
      Matcher matcher = pattern.matcher( statement );

      if( !matcher.find() )
        {
        LOG.warn( "did not match: {}", statement );
        continue;
        }

      int found = matcher.groupCount();

      for( int i = 0; i < found; i++ )
        command[ i ] = matcher.group( i + 1 );

      Action action = Action.valueOf( command[ 0 ].toUpperCase() );
      Table table = null;

      switch( action )
        {
        case DROP:
          table = new Table( action, command[ 2 ] );
          break;
        case CREATE:
          table = new Table( action, command[ 2 ], toColumns( command[ 3 ] ) );
          break;
        }

      commands.add( table );
      }

    return commands;
    }

  private Iterator<String> parseStatements( String string )
    {
    Scanner scanner = new Scanner( string ).useDelimiter( ";" );

    return Iterators.transform( scanner, new Function<String, String>()
    {
    @Override
    public String apply( String input )
      {
      if( input == null )
        return null;

      return input.replaceAll( "\n", "" );
      }
    } );
    }

  public static Column[] toColumns( String decl )
    {
    // ("supply_time" SMALLINT,   "store_cost" DECIMAL(10,4) NOT NULL, "unit_sales" DECIMAL(10,4) NOT NULL)
    decl = decl.replaceAll( "^\\((.*)\\)$", "$1" );

    // "supply_time" SMALLINT, "store_cost" DECIMAL(10,4) NOT NULL, "unit_sales" DECIMAL(10,4) NOT NULL
    String[] defs = decl.split( ",(?![\\d, ]+\\))" );
    for( int i = 0; i < defs.length; i++ )
      defs[ i ] = defs[ i ].trim().replaceAll( "\\s+", " " );

    // "supply_time" SMALLINT
    // "store_cost" DECIMAL(10,4) NOT NULL
    // "unit_sales" DECIMAL(10,4) NOT NULL
    Column[] columns = new Column[ defs.length ];
    for( int i = 0; i < defs.length; i++ )
      {
      String def = defs[ i ];
      String[] split = def.split( "\\s+", 2 );

      String name = split[ 0 ].replaceAll( "^\"(.*)\".*$", "$1" );
      Class type = getType( split[ 1 ] );
      columns[ i ] = new Column( name, type );
      }

    return columns;
    }

  private static Class getType( String string )
    {
    for( int i = 0; i < SqlTypeName.values().length; i++ )
      {
      SqlTypeName typeName = SqlTypeName.values()[ i ];

      String typeNameRegex = String.format( "^%s([\\s(].*|)$", typeName.toString() );
      boolean matches = string.toUpperCase().matches( typeNameRegex );
      boolean isNullable = !string.toUpperCase().contains( "NOT NULL" );

      if( matches )
        return getJavaTypeFor( typeName, isNullable );
      }

    throw new IllegalStateException( "type not found for: " + string );
    }

  private static Class getJavaTypeFor( SqlTypeName name, boolean isNullable )
    {
    switch( name )
      {
      case VARCHAR:
      case CHAR:
        return String.class;
      case INTEGER:
        return isNullable ? int.class : Integer.class;
      case BIGINT:
        return isNullable ? long.class : Long.class;
      case SMALLINT:
        return isNullable ? short.class : Short.class;
      case TINYINT:
        return isNullable ? byte.class : Byte.class;
      case REAL:
        return isNullable ? float.class : Float.class;
      case DECIMAL:
        return BigDecimal.class;
      case BOOLEAN:
        return isNullable ? boolean.class : Boolean.class;
      case BINARY:
      case VARBINARY:
        return ByteString.class;
      case DATE:
        return java.sql.Date.class;
      case TIME:
        return Time.class;
      case TIMESTAMP:
        return Timestamp.class;
      }

    throw new IllegalStateException( "unknown sql type name: " + name );
    }

  private static Fields toFields( Column[] columns )
    {
    Fields fields = Fields.NONE;

    for( Column column : columns )
      fields = fields.append( new Fields( column.name ).applyTypes( column.type ) );

    return fields;
    }
  }
