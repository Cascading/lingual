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

package cascading.lingual.tap;

import java.io.IOException;
import java.sql.SQLException;

import cascading.lingual.jdbc.LingualConnection;
import cascading.lingual.optiq.TapTable;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.util.Util;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.impl.java.MapSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TapSchema extends MapSchema
  {
  private static final Logger LOG = LoggerFactory.getLogger( TapSchema.class );

  private final PlatformBroker platformBroker;
  private final String name;
  private final String rootPath;

  public static TapSchema create( LingualConnection connection, String schemaIdentifier ) throws SQLException, IOException
    {
    MutableSchema rootSchema = connection.getRootSchema();
    TapSchema schema = new TapSchema( connection, Util.createSchemaNameFrom( schemaIdentifier ), schemaIdentifier );

    rootSchema.addSchema( schema.getName(), schema );

    return schema;
    }

  /**
   * Creates a MapSchema.
   *
   * @param connection Query provider
   * @param name
   * @param rootPath
   */
  public TapSchema( LingualConnection connection, String name, String rootPath ) throws IOException
    {
    super( connection.getParent(), connection.getTypeFactory(), makeExpression( connection, name ) );
    this.platformBroker = connection.getPlatformBroker();
    this.name = name;
    this.rootPath = rootPath;

    initialize();
    }

  public String getName()
    {
    return name;
    }

  private static Expression makeExpression( LingualConnection connection, String name )
    {
    return connection.getRootSchema().getSubSchemaExpression( name, Object.class );
    }

  protected void initialize() throws IOException
    {
    String[] childIdentifiers = platformBroker.getChildIdentifiers( rootPath );

    for( String identifier : childIdentifiers )
      createTapTable( identifier );
    }

  private void createTapTable( String identifier )
    {
    TapTable table = new TapTable( platformBroker, getQueryProvider(), this, identifier );

    LOG.info( "on schema: {}, adding table: {}, with fields: {}",
      new Object[]{getName(), table.getName(), table.getFields()} );

    addTable( table.getName(), table );
    }
  }
