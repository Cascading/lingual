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
import net.hydromatic.linq4j.expressions.BlockExpression;
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
import org.eigenbase.util.Permutation;
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

  static Branch resolveBranch( Stack stack, CascadingRelNode node, RexProgram program )
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

    boolean isPermutation = program.isPermutation();
    Permutation permutation = program.getPermutation();
    boolean isFilter = program.getCondition() != null;
    boolean isComplex = ProgramUtil.isComplex( program );
    boolean onlyProjectsNarrow = ProgramUtil.isOnlyProjectsNarrow( program );
    boolean hasConstants = ProgramUtil.hasConstants( program );
    boolean hasFunctions = ProgramUtil.hasFunctions( program );

    Pair<? extends Pipe, RelDataType> pair = Pair.of( branch.current, incomingRowType );
    if( isPermutation && !permutation.isIdentity() )
      {
      if( hasConstants || hasFunctions )
        throw new IllegalStateException( "permutation projection has constant and function transforms" );

      Fields permutationFields = createPermutationFields( incomingFields, permutation );

      pair = Pair.of( new Rename( pair.left, permutationFields, resultFields ), pair.right );
      }
    else
      {
      if( isFilter )
        pair = addFilter( cluster, program, pair.left, pair.right );

      if( isComplex )
        {
        pair = addFunction( cluster, program, pair.left, false, pair.right );
        }
      else
        {
        if( hasFunctions )
          pair = addFunction( cluster, program, pair.left, true, pair.right );

        if( hasConstants )
          pair = addConstants( node, program, pair.left, pair.right );

        boolean isRename = ProgramUtil.isOnlyRename( program );
        if( isRename )
          pair = addRename( cluster, program, pair.left, pair.right );

        if( onlyProjectsNarrow ) // discard constants etc
          resultFields = getNarrowFields( cluster, program );
        }
      }

    pair = addRetain( cluster, resultFields, pair.left, pair.right );

    Pipe pipe = stack.addDebug( node, pair.left );

    return new Branch( pipe, branch );
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

  private static Pair<Pipe, RelDataType> addRetain( RelOptCluster cluster, Fields resultFields, Pipe pipe, RelDataType incomingRowType )
    {
    Retain retain = new Retain( pipe, resultFields );
    RelDataType outRowType = incomingRowType; // FIXME
    return Pair.of( (Pipe) retain, outRowType );
    }

  private static Pair<Pipe, RelDataType> addRename( RelOptCluster cluster, RexProgram program, Pipe pipe, RelDataType incomingRowType )
    {
    boolean isRenameDuplicate = isRenameDuplicate( cluster, incomingRowType, program );
    boolean hasConstants = ProgramUtil.hasConstants( program );
    boolean hasFunctions = ProgramUtil.hasFunctions( program );
    final List<Integer> deletedFields;
    if( isRenameDuplicate && !( hasFunctions || hasConstants ) ) // are renaming into an existing field [city0->city]
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
      incomingRowType = outputRowType;
      }
    else
      {
      program = renameProgramInputFields( program, incomingRowType );
      deletedFields = Collections.emptyList();
      }

    // todo: remove identity renames
    RelDataType inputProjects = getInputProjectsRowType( program, incomingRowType, deletedFields );
    Fields incomingFields = createTypedFields( cluster, inputProjects );

    Fields renameFields = getNarrowFields( cluster, program );
    RelDataType renameRowType = incomingRowType; // FIXME;
    return Pair.of( (Pipe) new Rename( pipe, incomingFields, renameFields ), renameRowType );
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

  private static Pair<Pipe, RelDataType> addFilter( RelOptCluster cluster, RexProgram program, Pipe pipe, RelDataType incomingRowType )
    {
    final Fields incomingFields = createTypedFields( cluster, incomingRowType );
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
      return Pair.of( pipe, incomingRowType );

    // create a filter to remove records that don't meet the expression
    Expression nullToFalse = Expressions.call( Functions.class, "falseIfNull", condition );
    Expression not = Expressions.not( nullToFalse ); // matches #isRemove semantics in Filter

    statements.add( Expressions.return_( null, not ) );
    BlockExpression block = statements.toBlock();
    String expression = Expressions.toString( block );

    LOG.debug( "filter parameters: {}", incomingFields );
    LOG.debug( "filter expression: {}", expression );

    Filter expressionFilter = new ScriptFilter( expression, incomingFields.getTypesClasses() ); // handles coercions

    Each each = new Each( pipe, expressionFilter );
    return Pair.of( (Pipe) each, incomingRowType );
    }

  private static Pair<Pipe, RelDataType> addFunction( RelOptCluster cluster, RexProgram program, Pipe pipe, boolean narrow, RelDataType incomingRowType )
    {
    final Fields incomingFields = createTypedFields( cluster, incomingRowType );

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

    BlockExpression block = statements.toBlock();
    String expression = Expressions.toString( block );

    Fields outgoingFields = createTypedFields( cluster, program.getOutputRowType() );

    LOG.debug( "function parameters: {}", incomingRowType );
    LOG.debug( "function results: {}", outgoingFields );
    LOG.debug( "function expression: {}", expression );

    Function scriptFunction = new ScriptTupleFunction( outgoingFields, expression, incomingFields.getTypesClasses() );
    Fields outputSelector = narrow ? Fields.ALL : Fields.SWAP;

    return Pair.of( (Pipe) new Each( pipe, scriptFunction, outputSelector ), incomingRowType );
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
    if( new HashSet<String>( fieldNames ).size() < fieldNames.size() )
      throw new IllegalArgumentException( "field names are not unique in row type: " + rel.getRowType() );
    }
  }
