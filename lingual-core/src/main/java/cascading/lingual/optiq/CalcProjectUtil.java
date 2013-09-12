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
import java.util.List;

import cascading.lingual.optiq.meta.Branch;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Insert;
import cascading.operation.expression.ScriptFilter;
import cascading.operation.expression.ScriptTupleFunction;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.Splice;
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
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexProgram;
import org.eigenbase.rex.RexProgramBuilder;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Permutation;
import org.eigenbase.util.mapping.Mappings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.lingual.optiq.ProgramUtil.*;
import static cascading.lingual.optiq.RelUtil.createTypedFields;
import static cascading.lingual.optiq.RelUtil.createTypedFieldsSelector;

/**
 *
 */
class CalcProjectUtil
  {
  private static final Logger LOG = LoggerFactory.getLogger( CalcProjectUtil.class );

  static Branch resolveBranch( Stack stack, CascadingRelNode node, RexProgram program )
    {
    final RelOptCluster cluster = node.getCluster();

    CascadingRelNode child = (CascadingRelNode) ( (SingleRel) node ).getChild();
    Branch branch = child.visitChild( stack );
    Pipe pipe = branch.current;

    final List<String> names = getIncomingFieldNames( pipe );

    if( names != null && !names.equals( program.getInputRowType().getFieldNames() ) )
      program = renameInputs( program, cluster.getRexBuilder(), names );

    Split split = Split.of( program, cluster.getRexBuilder() );

    for( Pair<Op, RexProgram> pair : split.list )
      pipe = addProgram( cluster, pipe, pair.left, pair.right );

    pipe = stack.addDebug( node, pipe );

    return new Branch( pipe, branch );
    }

  private static List<String> getIncomingFieldNames( Pipe pipe )
    {
    if( pipe.getPrevious().length != 1 )
      return null;

    final Pipe previous = pipe.getPrevious()[ 0 ];

    if( !( previous instanceof Splice ) )
      return null;

    final Splice splice = (Splice) previous;

    if( splice.getDeclaredFields() == null )
      return null;

    return fieldNames( splice.getDeclaredFields() );
    }

  private static RexProgram renameInputs( RexProgram program, RexBuilder rexBuilder, List<String> names )
    {
    final RelDataType inputRowType = program.getInputRowType();
    if( inputRowType.getFieldNames().equals( names ) )
      return program;
    final RexProgramBuilder builder = RexProgramBuilder.create(
      rexBuilder,
      rexBuilder.getTypeFactory().createStructType(
        Pair.zip( names, RelOptUtil.getFieldTypeList( inputRowType ) ) ),
      program.getExprList(),
      program.getProjectList(),
      program.getCondition(),
      program.getOutputRowType(),
      false );
    return builder.getProgram();
    }

  private static List<String> fieldNames( Fields fields )
    {
    final List<String> names = new ArrayList<String>();
    for( Comparable field : fields )
      names.add( field.toString() );
    return names;
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
        return addFunction( cluster, program, pipe );
      default:
        throw new AssertionError( op ); // TODO:
      }
    }

  private static Pipe addRetain( RelOptCluster cluster, RexProgram program, Pipe pipe )
    {
    Fields resultFields = createTypedFields( cluster, program.getOutputRowType(), false );
    return new Retain( pipe, resultFields );
    }

  private static Pipe addRename( RelOptCluster cluster, RexProgram program, Pipe pipe )
    {
    // We know that the input has unique field names, and the output has unique
    // field names.
    if( !unique( program.getInputRowType().getFieldNames() ) )
      throw new AssertionError();

    if( !unique( program.getOutputRowType().getFieldNames() ) )
      throw new AssertionError();

    final Permutation permutation = program.getPermutation();

    if( permutation == null )
      throw new AssertionError();

    Fields incomingFields = createTypedFields( cluster, Mappings.apply( permutation.inverse(), program.getInputRowType().getFieldList() ), false );
    Fields renameFields = createTypedFieldsSelector( cluster, program.getOutputRowType(), false );

    return new Rename( pipe, incomingFields, renameFields );
    }

  private static Pair<Each, RelDataType> addConstants( CascadingRelNode node, RexProgram program, Pipe pipe, RelDataType incomingRowType )
    {
    RelDataType constantsRowType = ProgramUtil.getOutputConstantsRowType( program );
    Fields constantFields = createTypedFields( node.getCluster(), constantsRowType, false );
    List<RexLiteral> constantsLiterals = ProgramUtil.getOutputConstantsLiterals( program );
    List<Object> values = ProgramUtil.asValues2( constantsLiterals );

    RelDataType outRowType = incomingRowType; // FIXME
    Each each = new Each( pipe, Fields.NONE, new Insert( constantFields, values.toArray() ), Fields.ALL );
    return Pair.of( each, outRowType );
    }

  private static Pipe addFilter( RelOptCluster cluster, RexProgram program, Pipe pipe )
    {
    final Fields incomingFields = createTypedFields( cluster, program.getInputRowType(), false );
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

    return new Each( pipe, expressionFilter );
    }

  private static Pipe addFunction( RelOptCluster cluster, RexProgram program, Pipe pipe )
    {
    final Fields incomingFields = createTypedFields( cluster, program.getInputRowType(), false );

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

    Fields outgoingFields = createTypedFields( cluster, program.getOutputRowType(), false );

    LOG.debug( "function parameters: {}", program.getInputRowType() );
    LOG.debug( "function results: {}", outgoingFields );
    LOG.debug( "function expression: {}", expression );

    Function scriptFunction = new ScriptTupleFunction( outgoingFields, expression, incomingFields.getTypesClasses() );

    return new Each( pipe, scriptFunction, Fields.RESULTS );
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
  }
