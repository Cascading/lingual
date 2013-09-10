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
import java.util.HashSet;
import java.util.List;

import net.hydromatic.linq4j.Ord;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.reltype.RelRecordType;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexLocalRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexProgram;
import org.eigenbase.rex.RexProgramBuilder;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Permutation;

/**
 *
 */
class ProgramUtil
  {
  private static boolean hasFunctions( RexProgram program )
    {
    List<RexLocalRef> projects = program.getProjectList();
    RelDataType inputRowType = program.getInputRowType();
    int fieldCount = inputRowType.getFieldCount();

    for( RexLocalRef project : projects )
      {
      if( !program.isConstant( project ) && project.getIndex() >= fieldCount )
        return true;
      }

    return false;
    }

  private static boolean hasConstants( RexProgram program )
    {
    List<RexLocalRef> projects = program.getProjectList();

    for( RexLocalRef project : projects )
      {
      if( program.isConstant( project ) )
        return true;
      }

    return false;
    }

  private static boolean isOnlyRename( RexProgram program )
    {
    final List<String> inputFieldNames = program.getInputRowType().getFieldNames();
    final List<String> outputFieldNames = program.getOutputRowType().getFieldNames();

    return inputFieldNames.size() == outputFieldNames.size()
      && unique( inputFieldNames )
      && unique( outputFieldNames )
      && !inputFieldNames.equals( outputFieldNames );
    }

  /**
   * Returns whether a program returns a subset of its input fields. Fields
   * are in the same order, and no input field is output more than once.
   * There is no condition or non-trivial expressions.
   */
  private static boolean isRetain( RexProgram program, boolean allowRename )
    {
    if( program.getCondition() != null )
      return false;

    if( program.getExprList().size() > program.getInputRowType().getFieldCount() )
      return false;

    final HashSet<Integer> used = new HashSet<Integer>();

    for( Pair<RexLocalRef, String> pair : program.getNamedProjects() )
      {
      final int index = pair.left.getIndex();

      if( !used.add( index ) )
        return false; // field used more than once

      if( !allowRename && !program.getInputRowType().getFieldNames().get( index ).equals( pair.right ) )
        return false; // field projected with different name
      }

    return true;
    }

  private static boolean isComplex( RexProgram program )
    {
    final List<String> inputFieldNames = program.getInputRowType().getFieldNames();
    final List<String> outputFieldNames = program.getOutputRowType().getFieldNames();

    return !( inputFieldNames.size() == outputFieldNames.size()
      && unique( inputFieldNames )
      && unique( outputFieldNames ) );
    }

  public static RelDataType getOutputConstantsRowType( RexProgram program )
    {
    RelDataType outputRowType = program.getOutputRowType();
    List<RelDataTypeField> fields = new ArrayList<RelDataTypeField>();
    List<RexLocalRef> projectList = program.getProjectList();

    for( int i = 0; i < projectList.size(); i++ )
      {
      RexLocalRef ref = projectList.get( i );

      if( !program.isConstant( ref ) )
        continue;

      fields.add( outputRowType.getFieldList().get( i ) );
      }

    return new RelRecordType( fields );
    }

  public static List<RexLiteral> getOutputConstantsLiterals( RexProgram program )
    {
    List<RexNode> exprList = program.getExprList();
    List<RexLiteral> literals = new ArrayList<RexLiteral>();

    List<RexLocalRef> projectList = program.getProjectList();

    for( RexLocalRef ref : projectList )
      {
      if( !program.isConstant( ref ) )
        continue;

      literals.add( (RexLiteral) exprList.get( ref.getIndex() ) );
      }

    return literals;
    }

  public static List<Object> asValues2( List<RexLiteral> literals )
    {
    List<Object> values = new ArrayList<Object>();

    for( RexLiteral literal : literals )
      values.add( literal.getValue2() );

    return values;
    }

  static <E> boolean unique( List<E> elements )
    {
    return new HashSet<E>( elements ).size() == elements.size();
    }

  public static Analyzed analyze( RexProgram program )
    {
    return new Analyzed(
      program,
      hasFunctions( program ),
      hasConstants( program ),
      isComplex( program ),
      isOnlyRename( program ),
      program.getPermutation() );
    }

  static class Analyzed
    {
    private final RexProgram program;
    public final boolean hasFunctions;
    public final boolean hasConstants;
    public final boolean isComplex;
    public final boolean isOnlyRename;
    public final Permutation permutation;

    public Analyzed(
      RexProgram program,
      boolean hasFunctions,
      boolean hasConstants,
      boolean isComplex,
      boolean isOnlyRename,
      Permutation permutation )
      {
      this.program = program;
      this.hasFunctions = hasFunctions;
      this.hasConstants = hasConstants;
      this.isComplex = isComplex;
      this.isOnlyRename = isOnlyRename;
      this.permutation = permutation;
      }

    public boolean isFilter()
      {
      return program.getCondition() != null;
      }

    public boolean isRetainWithRename()
      {
      return ProgramUtil.isRetain( program, true );
      }

    public boolean isIdentity()
      {
      if( program.getCondition() != null || !program.getInputRowType().equals( program.getOutputRowType() ) )
        return false;

      for( int i = 0; i < program.getProjectList().size(); i++ )
        {
        RexLocalRef ref = program.getProjectList().get( i );

        if( ref.getIndex() != i )
          return false;
        }

      return true;
      }
    }

  /*
  if is non-trivial permutation
    add Rename
  else
    if has filter
      add ScriptFilter        # addFilter
    if is complex
      add ScriptTupleFunction # addFunction
    else
      if has functions
        add function
      if has constants
        add Insert (addConstants)
      if is rename
        if duplicate fields
          add Discard
        add Rename
  add Retain
  add Debug
   */

  enum Op
    {
      FILTER,
      FUNCTION,
      CONSTANT,
      DISCARD,
      RENAME,
      RETAIN
    }

  /**
   * A program decomposed into several programs, each of which can be
   * implemented in a single Cascading operation.
   */
  public static class Split
    {
    public final List<Pair<Op, RexProgram>> list;

    public Split( List<Pair<Op, RexProgram>> list )
      {
      this.list = list;

      /*
      assert filter == null
             || analyze( filter ).isPureFilter();
      assert function == null
             || analyze( function ).isPureFunction();
      assert constant == null
             || analyze( constant ).isPureConstant();
      assert discard == null
             || analyze( discard ).isPureDiscard();
      assert rename == null
             || analyze( rename ).isPureRename();
      assert retain == null
             || analyze( retain ).isPureRetain();
*/
      Pair<Op, RexProgram> previous = null;

      for( Pair<Op, RexProgram> pair : list )
        {
        if( previous != null )
          assert previous.right.getOutputRowType().equals( pair.right.getInputRowType() );

        previous = pair;
        }
      }

    public static Split of( RexProgram program, RexBuilder rexBuilder )
      {
      final RexProgram program0 = program; // for debug
      program = normalize( program, rexBuilder );
      final List<Pair<Op, RexProgram>> list = new ArrayList<Pair<Op, RexProgram>>();

      // Holds the previous link in the chain. The initial identity program is
      // not used, but is a convenient place to hold the row type.
      RexProgram previous = RexProgram.createIdentity( program.getInputRowType() );

      for( int count = 0; program != null; count++ )
        {
        // We rely on unique field names everywhere. Otherwise all bets are off.
        if( !unique( program.getInputRowType().getFieldNames() ) )
          throw new AssertionError();

        if( !unique( program.getOutputRowType().getFieldNames() ) )
          throw new AssertionError();

        if( program.equals( previous ) )
          break;

        // We should need no more than one call to each kind of operator (RENAME, RETAIN, etc.) so if
        // we're not done now, we'll never be.
        if( count > 10 )
          throw new AssertionError( "program cannot be simplified after " + count + " iterations:" + program );

        final Analyzed analyze = ProgramUtil.analyze( program );

        if( analyze.isIdentity() )
          break;

        if( analyze.permutation != null && !analyze.permutation.isIdentity() )
          {
          if( analyze.hasConstants || analyze.hasFunctions )
            throw new IllegalStateException( "permutation projection has constant and function transforms" );

          final RexProgramBuilder builder = new RexProgramBuilder( previous.getOutputRowType(), rexBuilder );

          for( int i = 0; i < analyze.permutation.getTargetCount(); i++ )
            {
            final int target = analyze.permutation.getTarget( i );
            builder.addProject( target, null );
            }

          previous = builder.getProgram();
          list.add( Pair.of( Op.RENAME, previous ) );
          break;
          }

        if( analyze.isFilter() )
          {
          // Build a program that has a condition (a possibly complex expression)
          // but projects all inputs.
          final RexProgramBuilder builder = new RexProgramBuilder( previous.getOutputRowType(), rexBuilder );
          builder.addIdentity();
          builder.addCondition( program.gatherExpr( program.getCondition() ) );
          previous = builder.getProgram();
          list.add( Pair.of( Op.FILTER, previous ) );

          // Remove condition from the remaining program.
          final RexProgramBuilder builder2 = RexProgramBuilder.forProgram( program, rexBuilder, false );
          builder2.clearCondition();
          program = builder2.getProgram( true );
          continue;
          }

        // TODO: remove "|| analyze.hasConstants" and generate a CONSTANTS slice
        if( analyze.isComplex || analyze.hasFunctions || analyze.hasConstants )
          {
          previous = program;
          list.add( Pair.of( Op.FUNCTION, previous ) );
          break;
          }

        if( analyze.hasConstants )
          {
          final RexProgramBuilder builder = new RexProgramBuilder( previous.getOutputRowType(), rexBuilder );

          if( true )
            throw new AssertionError(); // TODO:

          previous = builder.getProgram();
          list.add( Pair.of( Op.CONSTANT, previous ) );
          continue;
          }

        if( analyze.isOnlyRename )
          {
          // Create a program that projects all of its inputs, in order, but with different names.
          final RexProgramBuilder builder = new RexProgramBuilder( previous.getOutputRowType(), rexBuilder );
          final List<String> outputFieldNames = new ArrayList<String>( program.getInputRowType().getFieldNames() );

          for( Ord<String> name : Ord.zip( program.getOutputRowType().getFieldNames() ) )
            {
            final int source = program.getSourceField( name.i );

            if( source >= 0 )
              outputFieldNames.set( source, name.e );
            }

          for( int i = 0; i < outputFieldNames.size(); i++ )
            builder.addProject( i, outputFieldNames.get( i ) );

          previous = builder.getProgram();
          list.add( Pair.of( Op.RENAME, previous ) );

          // We're done. Remaining program would be the identity.
          break;
          }

        if( analyze.isRetainWithRename() )
          {
          final RexProgramBuilder builder = new RexProgramBuilder( previous.getOutputRowType(), rexBuilder );
          builder.addIdentity();
          builder.clearProjects();

          for( RexLocalRef pair : program.getProjectList() )
            {
            final int index = pair.getIndex();
            builder.addProject( index, program.getInputRowType().getFieldNames().get( index ) );
            }

          previous = builder.getProgram();
          list.add( Pair.of( Op.RETAIN, previous ) );

          // There may or may not be renames left.
          program = RexProgram.createIdentity( previous.getOutputRowType(), program.getOutputRowType() );

          continue;
          }

        throw new AssertionError( "program cannot be simplified: " + program );
        }

      return new Split( list );
      }
    }

  private static RexProgram normalize( RexProgram program, RexBuilder rexBuilder )
    {
    return RexProgramBuilder.forProgram( program, rexBuilder, true ).getProgram( false );
    }
  }
