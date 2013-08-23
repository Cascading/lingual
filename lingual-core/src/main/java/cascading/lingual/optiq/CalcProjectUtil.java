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

import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cascading.lingual.optiq.meta.Branch;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Insert;
import cascading.operation.expression.ScriptFilter;
import cascading.operation.expression.ScriptTupleFunction;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import net.hydromatic.linq4j.expressions.BlockBuilder;
import net.hydromatic.linq4j.expressions.BlockStatement;
import net.hydromatic.linq4j.expressions.ConstantExpression;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.rules.java.RexToLixTranslator;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexProgram;
import org.eigenbase.rex.RexProgramBuilder;
import org.eigenbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.lingual.optiq.ProgramUtil.*;
import static cascading.lingual.optiq.RelUtil.createPermutationFields;
import static cascading.lingual.optiq.RelUtil.createTypedFields;

/**
 *
 */
class CalcProjectUtil
  {
  private static final Logger LOG = LoggerFactory.getLogger( CalcProjectUtil.class );

  static Branch resolveBranch0( Stack stack, CascadingRelNode node, RexProgram program )
    {
    CascadingRelNode child = (CascadingRelNode) ( (SingleRel) node ).getChild();
    CalcProjectUtil.checkRowType( node );
    CalcProjectUtil.checkRowType( child );

    Branch branch = child.visitChild( stack );
    RelOptCluster cluster = node.getCluster();

    ///////////////////////
    // commented out code is for debugging, would leave in put some code
    // fails if these methods are called in all cases
    RelDataType incomingRowType = child.getRowType();
    Fields incomingFields = createTypedFields( cluster, incomingRowType );
//    Fields outgoingFields = createTypedFields( cluster, node.getRowType() );

//    Fields argumentFields = createTypedFields( cluster, program.getInputRowType() );
    Fields resultFields = createTypedFields( cluster, program.getOutputRowType() );

//    Fields inputProjects = createTypedFields( cluster, getInputProjectsRowType( program ) );
//    Fields outputProjects = createTypedFields( cluster, getOutputProjectsRowType( program ) );
    ///////////////////////

    final Analyzed analyze = ProgramUtil.analyze( program );

    Pair<? extends Pipe, RelDataType> pair = Pair.of( branch.current, incomingRowType );
    if( analyze.permutation != null && !analyze.permutation.isIdentity() )
      {
      if( analyze.hasConstants || analyze.hasFunctions )
        throw new IllegalStateException( "permutation projection has constant and function transforms" );

      Fields permutationFields = createPermutationFields( incomingFields, analyze.permutation );

      pair = Pair.of( new Rename( pair.left, permutationFields, resultFields ), pair.right );
      }
    else
      {
      if( analyze.isFilter() )
        {
        Pipe filter = addFilter( cluster, program, pair.left );
        pair = Pair.of( filter, null );
        }

      if( analyze.isComplex )
        {
        Pipe pipe = addFunction( cluster, program, pair.left, false );
        pair = Pair.of( pipe, null );
        }
      else
        {
        if( analyze.hasFunctions )
          {
          Pipe pipe = addFunction( cluster, program, pair.left, true );
          pair = Pair.of( pipe, null );
          }

        if( analyze.hasConstants )
          pair = addConstants( node, program, pair.left, pair.right );

        boolean isRename = analyze.isOnlyRename;
        if( isRename )
          {
          Pipe pipe = addRename( cluster, program, pair.left );
          pair = Pair.of( pipe, null );
          }

        if( analyze.onlyProjectsNarrow ) // discard constants etc
          resultFields = getNarrowFields( cluster, program );
        }
      }

    // TODO: create program that just projects resultFields
    RexProgram retainProgram = RexProgram.createIdentity( null );
    Pipe retain = addRetain( cluster, retainProgram, pair.left );
    pair = Pair.of( retain, null );

    Pipe pipe = stack.addDebug( node, pair.left );

    return new Branch( pipe, branch );
    }

  static Branch resolveBranch( Stack stack, CascadingRelNode node, RexProgram program )
    {
    final RelOptCluster cluster = node.getCluster();
    Split split = Split.of( program, cluster.getRexBuilder() );

    CascadingRelNode child = (CascadingRelNode) ( (SingleRel) node ).getChild();
    Branch branch = child.visitChild( stack );
    Pipe pipe = branch.current;
    for( Pair<Op, RexProgram> pair : split.list )
      {
      pipe = addProgram( cluster, pipe, pair.left, pair.right );
      }
    pipe = stack.addDebug( node, pipe );

    return new Branch( pipe, branch );
    }

  static Pipe addProgram( RelOptCluster cluster, Pipe pipe, Op op, RexProgram program )
    {
    switch( op )
      {
      case FILTER:
        return addFilter( cluster, program, pipe );
      case RETAIN:
        return addRetain( cluster, program, pipe );
      case RENAME:
        return addRename( cluster, program, pipe );
      case FUNCTION:
        return addFunction( cluster, program, pipe, false );
      default:
        throw new AssertionError( op ); // TODO:
      }
    }

  private static boolean isRenameDuplicate( RelOptCluster cluster, RelDataType incomingRowType, RexProgram program )
    {
    RelDataType outputProjects = removeIdentity( incomingRowType, program );
    Fields outputFields = createTypedFields( cluster, outputProjects );

    final Set<String> fieldNames = new HashSet<String>( incomingRowType.getFieldNames() );
    for( Comparable outputField : outputFields )
      {
      if( fieldNames.contains( outputField.toString() ) )
        return true;
      }

    return false;
    }

  private static Pipe addRetain( RelOptCluster cluster, RexProgram program, Pipe pipe )
    {
    assert ProgramUtil.analyze( program ).isPureRetain();
    Fields resultFields = createTypedFields( cluster, program.getOutputRowType() );
    return new Retain( pipe, resultFields );
    }

  private static Pipe addRename0( RelOptCluster cluster, RexProgram program, Pipe pipe )
    {
    final RelDataType incomingRowType = program.getInputRowType();
    boolean isRenameDuplicate = isRenameDuplicate( cluster, incomingRowType, program );
    final Analyzed analyze = ProgramUtil.analyze( program );
    final List<Integer> deletedFields;
    final RelDataType midRowType;
    if( isRenameDuplicate && !( analyze.hasFunctions || analyze.hasConstants ) ) // are renaming into an existing field [city0->city]
      {
      RelDataType outputProjects = removeIdentity( incomingRowType, program );
      RelDataType duplicatesRowType = getDuplicatesRowType( incomingRowType, outputProjects );
      Fields duplicatesFields = createTypedFields( cluster, duplicatesRowType );

      final Discard discard = new Discard( pipe, duplicatesFields );

      final Set<String> duplicateFieldNames = new HashSet<String>( duplicatesRowType.getFieldNames() );
      deletedFields = new ArrayList<Integer>();
      final List<RelDataTypeField> list = new ArrayList<RelDataTypeField>();
      for( RelDataTypeField field : incomingRowType.getFieldList() )
        {
        if( duplicateFieldNames.contains( field.getName() ) )
          deletedFields.add( field.getIndex() );
        else
          list.add( field );
        }
      RelDataType outputRowType = cluster.getTypeFactory().createStructType( list );

      pipe = discard;
      midRowType = outputRowType;
      }
    else
      {
      program = renameProgramInputFields( program, incomingRowType );
      deletedFields = Collections.emptyList();
      midRowType = incomingRowType;
      }

    // todo: remove identity renames
    RelDataType inputProjects = getInputProjectsRowType( program, midRowType, deletedFields );
    Fields incomingFields = createTypedFields( cluster, inputProjects );

    Fields renameFields = getNarrowFields( cluster, program );
    return new Rename( pipe, incomingFields, renameFields );
    }

  private static Pipe addRename( RelOptCluster cluster, RexProgram program, Pipe pipe )
    {
    // We know that the input has unique field names, and the output has unique
    // field names.
    if( !unique( program.getInputRowType().getFieldNames() ) )
      throw new AssertionError();
    if( !unique( program.getOutputRowType().getFieldNames() ) )
      throw new AssertionError();

    Fields incomingFields = createTypedFields( cluster, program.getInputRowType() );
    Fields renameFields = createTypedFields( cluster, program.getOutputRowType() );
    return new Rename( pipe, incomingFields, renameFields );
    }

  private static RexProgram renameProgramInputFields( RexProgram program, RelDataType incomingRowType )
    {
    if( incomingRowType.getFieldCount() != program.getInputRowType().getFieldCount() )
      throw new AssertionError();

    return new RexProgram( incomingRowType, program.getExprList(), program.getProjectList(), program.getCondition(), program.getOutputRowType() );
    }

  private static Fields getNarrowFields( RelOptCluster cluster, RexProgram program )
    {
    RelDataType outputProjectsRowType = getOutputProjectsRowType( program );

    return createTypedFields( cluster, outputProjectsRowType );
    }

  private static Pair<Each, RelDataType> addConstants( CascadingRelNode node, RexProgram program, Pipe pipe, RelDataType incomingRowType )
    {
    RelDataType constantsRowType = ProgramUtil.getOutputConstantsRowType( program );
    Fields constantFields = createTypedFields( node.getCluster(), constantsRowType );
    List<RexLiteral> constantsLiterals = ProgramUtil.getOutputConstantsLiterals( program );
    List<Object> values = ProgramUtil.asValues2( constantsLiterals );

    RelDataType outRowType = incomingRowType; // FIXME
    Each each = new Each( pipe, Fields.NONE, new Insert( constantFields, values.toArray() ), Fields.ALL );
    return Pair.of( each, outRowType );
    }

  private static Pipe addFilter( RelOptCluster cluster, RexProgram program, Pipe pipe )
    {
    final Fields incomingFields = createTypedFields( cluster, program.getInputRowType() );
    BlockBuilder statements = new BlockBuilder();

    Expression condition = RexToLixTranslator.translateCondition(
      program,
      (JavaTypeFactory) cluster.getTypeFactory(),
      statements,
      new RexToLixTranslator.InputGetter()
      {
      public Expression field( BlockBuilder list, int index )
        {
        return Expressions.parameter( incomingFields.getType( index ), incomingFields.get( index ).toString() );
        }
      } );

    // if condition is constant and true, we don't need an expression filter to keep it around
    boolean keepsAllRecords = condition instanceof ConstantExpression && Boolean.TRUE.equals( ( (ConstantExpression) condition ).value );

    if( keepsAllRecords )
      return pipe;

    // create a filter to remove records that don't meet the expression
    Expression nullToFalse = Expressions.call( Functions.class, "falseIfNull", condition );
    Expression not = Expressions.not( nullToFalse ); // matches #isRemove semantics in Filter

    statements.add( Expressions.return_( null, not ) );
    BlockStatement block = statements.toBlock();
    String expression = Expressions.toString( block );

    LOG.debug( "filter parameters: {}", incomingFields );
    LOG.debug( "filter expression: {}", expression );

    Filter expressionFilter = new ScriptFilter( expression, incomingFields.getTypesClasses() ); // handles coercions

    Each each = new Each( pipe, expressionFilter );
    return each;
    }

  private static Pipe addFunction( RelOptCluster cluster, RexProgram program, Pipe pipe, boolean narrow )
    {
    final Fields incomingFields = createTypedFields( cluster, program.getInputRowType() );

    // only project the result of any expressions
    if( narrow )
      program = ProgramUtil.createNarrowProgram( program, cluster.getRexBuilder() );

    BlockBuilder statements = new BlockBuilder();

    List<Expression> expressionList = RexToLixTranslator.translateProjects(
      program,
      (JavaTypeFactory) cluster.getTypeFactory(),
      statements,
      new RexToLixTranslator.InputGetter()
      {
      public Expression field( BlockBuilder list, int index )
        {
        final Type type = incomingFields.getType( index );
        final String name = incomingFields.get( index ).toString();
        return Expressions.parameter( type, name );
        }
      } );

    Expression record = Expressions.newArrayInit( Object.class, expressionList );

    record = Expressions.new_( getConstructor(), record );

    statements.add( Expressions.return_( null, record ) );

    BlockStatement block = statements.toBlock();
    String expression = Expressions.toString( block );

    Fields outgoingFields = createTypedFields( cluster, program.getOutputRowType() );

    LOG.debug( "function parameters: {}", program.getInputRowType() );
    LOG.debug( "function results: {}", outgoingFields );
    LOG.debug( "function expression: {}", expression );

    Function scriptFunction = new ScriptTupleFunction( outgoingFields, expression, incomingFields.getTypesClasses() );
    Fields outputSelector = narrow ? Fields.ALL : Fields.SWAP;

    return new Each( pipe, scriptFunction, outputSelector );
    }

  private static Constructor<Tuple> getConstructor()
    {
    try
      {
      return Tuple.class.getConstructor( Object[].class );
      }
    catch( NoSuchMethodException exception )
      {
      LOG.error( "unable to get constructor for Tuple" );
      throw new RuntimeException( exception );
      }
    }

  public static RexProgram createRexProgram( CascadingProjectRel projectRel )
    {
    RelOptCluster cluster = projectRel.getCluster();
    RelDataType rowType = projectRel.getChild().getRowType();

    RexProgramBuilder builder = new RexProgramBuilder( rowType, cluster.getRexBuilder() );

    List<Pair<RexNode, String>> projects = projectRel.getNamedProjects();

    for( int i = 0; i < projects.size(); i++ )
      {
      Pair<RexNode, String> exp = projects.get( i );

      if( !( exp.left instanceof RexInputRef ) ) // RexCall or RexLiteral
        builder.addExpr( exp.left );

      int index = i;

      if( exp.left instanceof RexInputRef )
        index = ( (RexInputRef) exp.left ).getIndex();

      builder.addProject( index, exp.right );
      }

    return builder.getProgram( false ); // todo: optimizer causes issues
    }

  public static void checkRowType( RelNode rel )
    {
    final List<String> fieldNames = rel.getRowType().getFieldNames();
    if( !unique( fieldNames ) )
      throw new IllegalArgumentException( "field names are not unique in row type: " + rel.getRowType() );
    }
  }
