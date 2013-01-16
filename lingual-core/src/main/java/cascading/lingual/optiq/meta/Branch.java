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

import java.util.Map;

import cascading.lingual.platform.PlatformBroker;
import cascading.pipe.Pipe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Branch
  {
  private static final Logger LOG = LoggerFactory.getLogger( Branch.class );

  public PlatformBroker platformBroker;
  public Map<Ref, Pipe> heads;
  public Pipe current;
  public Ref tail;
  public boolean isModification = false;

  public Branch( PlatformBroker platformBroker, Map<Ref, Pipe> heads, String name, String identifier )
    {
    this.platformBroker = platformBroker;
    this.heads = heads;

    Ref head = new Ref( name, identifier );

    if( this.heads.containsKey( head ) )
      {
      LOG.debug( "re-using branch head: {}, for: {}", name, identifier );

      this.current = this.heads.get( head );
      }
    else
      {
      LOG.debug( "adding branch head: {}, for: {}", name, identifier );

      this.current = new Pipe( name );
      this.heads.put( head, this.current );
      }
    }

  public Branch( Branch branch, String name, String identifier )
    {
    LOG.debug( "adding branch tail: {}, for: {}", name, identifier );

    this.platformBroker = branch.platformBroker;
    this.heads = branch.heads;
    this.current = new Pipe( name, branch.current );
    this.tail = new Ref( name, identifier );
    this.isModification = true;
    }

  public Branch( Pipe current, Branch... branches )
    {
    LOG.debug( "adding branch: {}", current.getName() );

    this.current = current;
    this.heads = branches[ 0 ].heads; // all shared

    for( Branch branch : branches )
      {
      if( platformBroker == null )
        platformBroker = branch.platformBroker;
      else if( platformBroker != branch.platformBroker )
        throw new IllegalStateException( "diff instances of properties found in branches" );
      }
    }
  }
