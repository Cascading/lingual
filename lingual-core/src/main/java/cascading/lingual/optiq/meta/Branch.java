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

package cascading.lingual.optiq.meta;

import java.util.List;
import java.util.Map;

import cascading.lingual.catalog.TableDef;
import cascading.lingual.platform.PlatformBroker;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import org.eigenbase.rex.RexLiteral;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Branch
  {
  private static final Logger LOG = LoggerFactory.getLogger( Branch.class );

  public PlatformBroker platformBroker;
  public List<List<RexLiteral>> tuples;
  public Map<Ref, Pipe> heads;
  public TableDef tailTableDef;
  public Pipe current;
  public boolean isModification = false;

  public Branch( PlatformBroker platformBroker, Map<Ref, Pipe> heads, String name, TableDef tableDef )
    {
    this.platformBroker = platformBroker;
    this.heads = heads;

    Ref head = new Ref( name, tableDef );

    if( this.heads.containsKey( head ) )
      {
      LOG.debug( "re-using branch head: {}, for: {}", name, tableDef );

      this.current = this.heads.get( head );
      }
    else
      {
      LOG.debug( "adding branch head: {}, for: {}", name, tableDef );

      this.current = new Pipe( name );
      this.heads.put( head, this.current );
      }
    }

  public Branch( PlatformBroker platformBroker, Branch branch, TableDef tableDef )
    {
    LOG.debug( "adding branch tail: {}, for table: {}", tableDef.getName(), tableDef.getName() );

    this.platformBroker = platformBroker;
    this.heads = branch.heads;
    this.tailTableDef = tableDef;
    this.current = new Pipe( tableDef.getName(), branch.current );
    this.isModification = true;
    this.tuples = branch.tuples;
    }

  public Branch( Pipe current, Branch... branches )
    {
    LOG.debug( "adding branch: {}", current.getName() );

    this.current = current;
    this.heads = branches[ 0 ].heads; // all shared

    for( Branch branch : branches )
      {
      if( platformBroker == null && branch.platformBroker != null )
        platformBroker = branch.platformBroker;
      else if( platformBroker != branch.platformBroker && branch.platformBroker != null )
        throw new IllegalStateException( "diff instances of properties found in branches" );
      }
    }

  public Branch( PlatformBroker platformBroker, TableDef tableDef, List<List<RexLiteral>> tuples )
    {
    LOG.debug( "adding values insertion" );

    this.platformBroker = platformBroker;
    this.tuples = tuples;
    this.tailTableDef = tableDef;
    this.isModification = true;
    }

  public Branch( Map<Ref, Pipe> heads, String name, Fields fields, List<List<RexLiteral>> tuples )
    {
    LOG.debug( "adding values" );

    this.heads = heads;
    this.tuples = tuples;

    Ref head = new Ref( name, fields, tuples );

    if( this.heads.containsKey( head ) )
      {
      LOG.debug( "re-using branch head: {}", name );

      this.current = this.heads.get( head );
      }
    else
      {
      LOG.debug( "adding branch head: {}", name );

      this.current = new Pipe( name );
      this.heads.put( head, this.current );
      }
    }
  }
