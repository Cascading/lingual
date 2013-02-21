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

package cascading.lingual.optiq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import cascading.lingual.optiq.meta.Branch;
import cascading.pipe.CoGroup;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.AverageBy;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.MaxBy;
import cascading.pipe.assembly.MinBy;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.SumBy;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.InnerJoin;
import cascading.tuple.Fields;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.util.Util;

import static cascading.lingual.optiq.RelUtil.getTypedFields;
import static cascading.lingual.optiq.RelUtil.getTypedFieldsFor;

/**
 *
 */
public class CascadingAggregateRel extends AggregateRelBase implements CascadingRelNode
  {
  public CascadingAggregateRel( RelOptCluster cluster, RelTraitSet traitSet, RelNode child, BitSet groupSet, List<AggregateCall> aggCallList )
    {
    super( cluster, traitSet.plus( Cascading.CONVENTION ), child, groupSet, aggCallList );
    }

  @Override
  public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs )
    {
    return new CascadingAggregateRel( getCluster(), getTraitSet(), sole( inputs ), getGroupSet(), getAggCallList() );
    }

  @Override
  public RelOptCost computeSelfCost( RelOptPlanner planner )
    {
    return super.computeSelfCost( planner ).multiplyBy( .1 );
    }

  @Override
  public Branch visitChild( Stack stack )
    {
    RelNode child = getChild();
    Branch branch = ( (CascadingRelNode) child ).visitChild( stack );

    // assumption here is if aggCalls is empty, we are performing a DISTINCT on the group set
    if( isDistinct() )
      {
      Pipe current = new Unique( branch.current, getTypedFieldsFor( this ) );

      current = stack.addDebug( this, current );

      return new Branch( current, branch );
      }

    RelDataType inputRowType = getInput( 0 ).getRowType();

    Pipe previous = branch.current;
    Fields groupFields = getTypedFields( getCluster(), inputRowType, Util.toIter( getGroupSet() ) );

    List<AggregateCall> distincts = getDistincts();
    List<AggregateCall> concurrents = getConcurrents( distincts );

    AggregateBy concurrentAggregates = createConcurrentAggregates( inputRowType, previous, groupFields, concurrents );
    Pipe[] distinctAggregates = createDistinctAggregates( stack, inputRowType, previous, groupFields, distincts );

    if( concurrentAggregates == null && distinctAggregates == null )
      throw new IllegalStateException( "concurrent and distinct aggregates are null" );

    if( concurrentAggregates != null && distinctAggregates == null )
      return new Branch( stack.addDebug( this, concurrentAggregates ), branch );

    if( concurrentAggregates == null && distinctAggregates != null && distinctAggregates.length == 1 )
      return new Branch( stack.addDebug( this, distinctAggregates[ 0 ] ), branch );

    Pipe[] pipes = createPipes( concurrentAggregates, distinctAggregates );
    Fields declaredFields = createDeclaredFields( groupFields, distincts, concurrentAggregates );
    Fields[] groupFieldsArray = createGroupingFields( groupFields, pipes );

    Pipe join;

    if( groupFields.isNone() ) // not grouping, just appending tuples into a single row
      join = new HashJoin( pipes, groupFieldsArray, declaredFields, new InnerJoin() );
    else
      join = new CoGroup( pipes, groupFieldsArray, declaredFields, new InnerJoin() );

    join = stack.addDebug( this, join );

    return new Branch( join, branch );
    }

  private Fields[] createGroupingFields( Fields groupFields, Pipe[] pipes )
    {
    Fields[] groupFieldsArray = new Fields[ pipes.length ];

    Arrays.fill( groupFieldsArray, groupFields );
    return groupFieldsArray;
    }

  private Pipe[] createPipes( AggregateBy concurrentAggregates, Pipe[] distinctAggregates )
    {
    List<Pipe> pipeList = new ArrayList<Pipe>();

    if( concurrentAggregates != null )
      pipeList.add( concurrentAggregates );

    Collections.addAll( pipeList, distinctAggregates );

    return pipeList.toArray( new Pipe[ pipeList.size() ] );
    }

  private Fields createDeclaredFields( Fields groupFields, List<AggregateCall> distincts, AggregateBy concurrentAggregates )
    {
    List<Fields> declaredFieldsList = new ArrayList<Fields>();

    declaredFieldsList.add( groupFields );

    if( concurrentAggregates != null )
      Collections.addAll( declaredFieldsList, concurrentAggregates.getFieldDeclarations() );

    Collections.addAll( declaredFieldsList, makeFieldsFor( distincts ) );

    return Fields.join( declaredFieldsList.toArray( new Fields[ declaredFieldsList.size() ] ) );
    }

  private Pipe[] createDistinctAggregates( Stack stack, RelDataType inputRowType, Pipe previous, Fields groupFields, List<AggregateCall> distincts )
    {
    if( distincts.isEmpty() )
      return null;

    List<Pipe> aggregates = new ArrayList<Pipe>();

    for( AggregateCall aggCall : distincts )
      {
      String aggregationName = aggCall.getAggregation().getName();
      Fields argFields = getTypedFields( getCluster(), inputRowType, aggCall.getArgList() );

      if( argFields.equals( Fields.NONE ) )
        argFields = Fields.ALL;

      Fields uniqueFields = argFields;

      if( !groupFields.equals( Fields.NONE ) )
        uniqueFields = groupFields.append( uniqueFields );

      Fields aggResultFields = makeFieldsFor( aggCall );

      Pipe current = previous;

      current = new Retain( current, uniqueFields );
      current = new Unique( aggResultFields.toString(), current, uniqueFields, Unique.Include.NO_NULLS );

      current = stack.addDebug( this, current );

      if( aggregationName.equals( "COUNT" ) )
        aggregates.add( makeCount( current, groupFields, argFields, aggResultFields ) );
      else if( aggregationName.equals( "SUM" ) )
        aggregates.add( makeSum( current, groupFields, argFields, aggResultFields ) );
      else if( aggregationName.equals( "MIN" ) )
        aggregates.add( makeMin( current, groupFields, argFields, aggResultFields ) );
      else if( aggregationName.equals( "MAX" ) )
        aggregates.add( makeMax( current, groupFields, argFields, aggResultFields ) );
      else if( aggregationName.equals( "AVG" ) )
        aggregates.add( makeAvg( current, groupFields, argFields, aggResultFields ) );
      else
        throw new UnsupportedOperationException( "unimplemented aggregation: " + aggregationName );
      }

    return aggregates.toArray( new Pipe[ aggregates.size() ] );
    }

  private Pipe makeCount( Pipe unique, Fields groupFields, Fields argFields, Fields resultFields )
    {
    return new CountBy( unique, groupFields, argFields, resultFields, CountBy.Include.NO_NULLS );
    }

  private Pipe makeSum( Pipe unique, Fields groupFields, Fields argFields, Fields resultFields )
    {
    return new SumBy( unique, groupFields, argFields, resultFields );
    }

  private Pipe makeMin( Pipe unique, Fields groupFields, Fields argFields, Fields resultFields )
    {
    return new MinBy( unique, groupFields, argFields, resultFields );
    }

  private Pipe makeMax( Pipe unique, Fields groupFields, Fields argFields, Fields resultFields )
    {
    return new MaxBy( unique, groupFields, argFields, resultFields );
    }

  private Pipe makeAvg( Pipe unique, Fields groupFields, Fields argFields, Fields resultFields )
    {
    return new AverageBy( unique, groupFields, argFields, resultFields, AverageBy.Include.NO_NULLS );
    }

  private AggregateBy createConcurrentAggregates( RelDataType inputRowType, Pipe previous, Fields groupFields, List<AggregateCall> concurrents )
    {
    if( concurrents.isEmpty() )
      return null;

    List<AggregateBy> aggregates = new ArrayList<AggregateBy>();

    for( AggregateCall aggCall : concurrents )
      {
      String aggregationName = aggCall.getAggregation().getName();
      Fields argFields = getTypedFields( getCluster(), inputRowType, aggCall.getArgList() );

      if( argFields.equals( Fields.NONE ) )
        argFields = Fields.ALL;

      Fields aggResultFields = makeFieldsFor( aggCall );

      if( aggregationName.equals( "COUNT" ) )
        aggregates.add( makeCountBy( argFields, aggResultFields ) );
      else if( aggregationName.equals( "SUM" ) )
        aggregates.add( makeSumBy( argFields, aggResultFields ) );
      else if( aggregationName.equals( "MIN" ) )
        aggregates.add( makeMinBy( argFields, aggResultFields ) );
      else if( aggregationName.equals( "MAX" ) )
        aggregates.add( makeMaxBy( argFields, aggResultFields ) );
      else if( aggregationName.equals( "AVG" ) )
        aggregates.add( makeAvgBy( argFields, aggResultFields ) );
      else
        throw new UnsupportedOperationException( "unimplemented aggregation: " + aggregationName );
      }

    return new AggregateBy( previous, groupFields, aggregates.toArray( new AggregateBy[ aggregates.size() ] ) );
    }

  private List<AggregateCall> getConcurrents( List<AggregateCall> distincts )
    {
    List<AggregateCall> concurrent = new ArrayList<AggregateCall>( aggCalls );

    concurrent.removeAll( distincts );

    return concurrent;
    }

  private List<AggregateCall> getDistincts()
    {
    List<AggregateCall> distincts = new ArrayList<AggregateCall>();

    for( AggregateCall aggCall : aggCalls )
      {
      if( aggCall.isDistinct() )
        distincts.add( aggCall );
      }

    return distincts;
    }

  private AggregateBy makeMaxBy( Fields argFields, Fields resultFields )
    {
    return new MaxBy( argFields, resultFields );
    }

  private AggregateBy makeMinBy( Fields argFields, Fields resultFields )
    {
    return new MinBy( argFields, resultFields );
    }

  private AggregateBy makeAvgBy( Fields argFields, Fields resultFields )
    {
    return new AverageBy( argFields, resultFields, AverageBy.Include.NO_NULLS );
    }

  private AggregateBy makeSumBy( Fields argFields, Fields resultFields )
    {
    return new SumBy( argFields, resultFields );
    }

  private AggregateBy makeCountBy( Fields argFields, Fields resultFields )
    {
    return new CountBy( argFields, resultFields, CountBy.Include.NO_NULLS );
    }

  private Fields[] makeFieldsFor( List<AggregateCall> aggCalls )
    {
    Fields[] fields = new Fields[ aggCalls.size() ];

    for( int i = 0; i < aggCalls.size(); i++ )
      fields[ i ] = makeFieldsFor( aggCalls.get( i ) );

    return fields;
    }

  private Fields makeFieldsFor( AggregateCall aggCall )
    {
    return new Fields( aggCall.getName(), RelUtil.getJavaType( getCluster(), aggCall.getType() ) );
    }
  }
