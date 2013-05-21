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

package cascading.lingual.catalog.ddl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.Charset;
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
import cascading.lingual.type.SQLDateCoercibleType;
import cascading.lingual.type.SQLTimeCoercibleType;
import cascading.lingual.type.SQLTimestampCoercibleType;
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
  private final String defaultExtension;

  public DDLParser( SchemaCatalog catalog, String schemaName, String protocol, String format )
    {
    this( catalog, schemaName, null, protocol, format );
    }

  public DDLParser( SchemaCatalog catalog, String schemaName, String schemaPath, String protocol, String format )
    {
    this( catalog, schemaName, schemaPath, protocol, format, null );
    }

  public DDLParser( SchemaCatalog catalog, String schemaName, String schemaPath, String protocol, String format, String defaultExtension )
    {
    this.catalog = catalog;
    this.schemaName = schemaName;
    this.schemaPath = schemaPath == null ? getSchemaIdentifier( catalog, schemaName ) : schemaPath;
    this.protocol = Protocol.getProtocol( protocol );
    this.format = Format.getFormat( format );
    this.defaultExtension = defaultExtension;
    }

  private String getSchemaIdentifier( SchemaCatalog catalog, String schemaName )
    {
    SchemaDef schemaDef = catalog.getSchemaDef( schemaName );

    if( schemaDef == null )
      throw new IllegalArgumentException( "schema does not exist: " + schemaName );

    String identifier = schemaDef.getIdentifier();

    if( identifier != null )
      return identifier;

    return schemaName;
    }

  public void apply( File file ) throws IOException
    {
    execute( parse( file ) );
    }

  public void execute( List<DDLTable> commands )
    {
    verifySchemaDef();

    for( DDLTable command : commands )
      {
      String name = command.name;
      DDLColumn[] ddlColumns = command.ddlColumns;

      switch( command.ddlAction )
        {
        case DROP:
          catalog.removeTableDef( schemaName, name );
          catalog.removeStereotype( schemaName, name );
          break;
        case CREATE:
          String stereotypeName = name;
          Fields fields = toFields( ddlColumns );
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

  private void verifySchemaDef()
    {
    SchemaDef schemaDef = catalog.getSchemaDef( schemaName );

    if( schemaDef == null )
      throw new IllegalStateException( "schema does not exist: " + schemaName );
    else if( schemaDef.getIdentifier() != null && !schemaPath.equals( schemaDef.getIdentifier() ) )
      throw new IllegalStateException( "schema already exists with identifier: " + schemaDef.getIdentifier() );
    }

  private String createTableIdentifier( String name )
    {
    String result = getSchemaIdentifier( catalog, schemaName ) + "/" + name;

    if( defaultExtension != null )
      result += "." + defaultExtension;

    return result;
    }

  public List<DDLTable> parse( InputStream inputStream ) throws IOException
    {
    InputStreamReader stream = new InputStreamReader( inputStream );

    return parse( CharStreams.toString( stream ) );
    }

  public List<DDLTable> parse( File file ) throws IOException
    {
    return parse( Files.toString( file, Charset.forName( "UTF-8" ) ) );
    }

  public List<DDLTable> parse( String string )
    {
    Iterator<String> statements = parseStatements( string );

    List<DDLTable> commands = new ArrayList<DDLTable>();

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

      DDLAction ddlAction = DDLAction.valueOf( command[ 0 ].toUpperCase() );
      DDLTable ddlTable = null;

      switch( ddlAction )
        {
        case DROP:
          ddlTable = new DDLTable( ddlAction, command[ 2 ] );
          break;
        case CREATE:
          ddlTable = new DDLTable( ddlAction, command[ 2 ], toColumns( command[ 3 ] ) );
          break;
        }

      commands.add( ddlTable );
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

  public static DDLColumn[] toColumns( String decl )
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
    DDLColumn[] DDLColumns = new DDLColumn[ defs.length ];
    for( int i = 0; i < defs.length; i++ )
      {
      String def = defs[ i ];
      String[] split = def.split( "\\s+", 2 );

      String name = split[ 0 ].replaceAll( "^\"(.*)\".*$", "$1" );
      Type type = getType( split[ 1 ] );
      DDLColumns[ i ] = new DDLColumn( name, type );
      }

    return DDLColumns;
    }

  private static Type getType( String string )
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

  private static Type getJavaTypeFor( SqlTypeName name, boolean isNullable )
    {
    switch( name )
      {
      case VARCHAR:
      case CHAR:
        return String.class;
      case INTEGER:
        return isNullable ? Integer.class : int.class;
      case BIGINT:
        return isNullable ? Long.class : long.class;
      case SMALLINT:
        return isNullable ? Short.class : short.class;
      case TINYINT:
        return isNullable ? Byte.class : byte.class;
      case REAL:
        return isNullable ? Float.class : float.class;
      case DECIMAL:
        return BigDecimal.class;
      case BOOLEAN:
        return isNullable ? Boolean.class : boolean.class;
      case BINARY:
      case VARBINARY:
        return ByteString.class;
      case DATE:
        return new SQLDateCoercibleType();
      case TIME:
        return new SQLTimeCoercibleType();
      case TIMESTAMP:
        return new SQLTimestampCoercibleType();
      }

    throw new IllegalStateException( "unknown sql type name: " + name );
    }

  private static Fields toFields( DDLColumn[] ddlColumns )
    {
    Fields fields = Fields.NONE;

    for( DDLColumn ddlColumn : ddlColumns )
      fields = fields.append( new Fields( ddlColumn.name ).applyTypes( ddlColumn.type ) );

    return fields;
    }
  }
