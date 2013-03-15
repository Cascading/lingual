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
import java.util.List;

import cascading.lingual.optiq.meta.Branch;
import cascading.operation.Filter;
import cascading.operation.Insert;
import cascading.operation.expression.ScriptFilter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import net.hydromatic.linq4j.expressions.BlockBuilder;
import net.hydromatic.linq4j.expressions.BlockExpression;
import net.hydromatic.linq4j.expressions.ConstantExpression;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.rules.java.RexToLixTranslator;
import org.eigenbase.rel.CalcRelBase;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexLocalRef;
import org.eigenbase.rex.RexProgram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.lingual.optiq.RelUtil.createTypedFields;
import static cascading.lingual.optiq.RelUtil.createTypedFieldsFor;
import static org.eigenbase.relopt.RelOptUtil.getFieldNames;

/**
 *
 */
public class CascadingCalcRel extends CalcRelBase implements CascadingRelNode
  {
  private static final Logger LOG = LoggerFactory.getLogger( CascadingCalcRel.class );

  public CascadingCalcRel( RelOptCluster cluster, RelTraitSet traits, RelNode child, RelDataType rowType, RexProgram program, List<RelCollation> collationList )
    {
    super( cluster, traits.plus( Cascading.CONVENTION ), child, rowType, program, collationList );
    }

  @Override
  public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs )
    {
    return new CascadingCalcRel( getCluster(), getTraitSet(), sole( inputs ), getRowType(), getProgram(), getCollationList() );
    }

  @Override
  public RelOptCost computeSelfCost( RelOptPlanner planner )
    {
    return super.computeSelfCost( planner ).multiplyBy( .1 );
    }

  @Override
  public Branch visitChild( Stack stack )
    {
    Branch branch = ( (CascadingRelNode) getChild() ).visitChild( stack );
    Pipe pipe = branch.current;
    Fields fields = createTypedFields( getChild(), getProgram().getExprList() );

    final List<Expression> parameters = new ArrayList<Expression>();

    for( int i = 0; i < fields.size(); i++ )
      parameters.add( Expressions.parameter( fields.getType( i ), fields.get( i ).toString() ) );

    BlockBuilder statements = new BlockBuilder();

    Expression condition = RexToLixTranslator.translateCondition(
      program,
      (JavaTypeFactory) getCluster().getTypeFactory(),
      statements,
      new RexToLixTranslator.InputGetter()
      {
      public Expression field( BlockBuilder list, int index )
        {
        return parameters.get( index );
        }
      } );

    // if condition is constant and true, we don't need an expression filter to keep it around
    boolean isConstantTrue = condition instanceof ConstantExpression && Boolean.TRUE.equals( ( (ConstantExpression) condition ).value );

    if( !isConstantTrue )
      {
      // create a filter to remove records that doesn't meet the expression

      Expression nullToFalse = Expressions.call( CascadingCalcRel.class, "falseIfNull", condition );
      Expression not = Expressions.not( nullToFalse ); // matches #isRemove semantics in Filter

      statements.add( Expressions.return_( null, not ) );
      BlockExpression block = statements.toBlock();
      String expression = Expressions.toString( block );

      LOG.debug( "calc expression: {}", expression );

      Filter expressionFilter = new ScriptFilter( expression, getFieldNames( getChild().getRowType() ), fields.getTypesClasses() );
      pipe = new Each( pipe, fields, expressionFilter );
      }

    // should we keep all the fields in their natural order
    boolean projectsAllInputFields = program.projectsIdentity( false );

    // we need to narrow/order the input fields
    if( !projectsAllInputFields )
      {
      Fields outgoingFields = createTypedFields( getCluster(), program.getOutputRowType() );

      List<RexLocalRef> projectList = program.getProjectList();
      Fields constantFields = Fields.NONE;
      List<Object> constantValues = new ArrayList<Object>();

      // simply accumulate constant values that compose the output columns
      for( int i = 0; i < projectList.size(); i++ )
        {
        RexLocalRef ref = projectList.get( i );

        if( !program.isConstant( ref ) )
          continue;

        RelDataTypeField relDataTypeField = program.getOutputRowType().getFields()[ i ];
        constantFields = constantFields.append( createTypedFieldsFor( getCluster(), relDataTypeField ) );

        RexLiteral node = (RexLiteral) program.getExprList().get( ref.getIndex() );
        constantValues.add( node.getValue2() );
        }

      // if no constant values, simply narrow the pipe
      // otherwise insert the values and narrow the pipe
      if( constantValues.isEmpty() )
        pipe = new Retain( pipe, outgoingFields );
      else
        pipe = new Each( pipe, new Insert( constantFields, constantValues.toArray( new Object[ 0 ] ) ), outgoingFields );
      }

    if( !isConstantTrue || !projectsAllInputFields )
      pipe = stack.addDebug( this, pipe );

    return new Branch( pipe, branch );
    }

  /**
   * called by the above expression
   *
   * @param result Boolean value
   * @return False if input is false or null
   */
  public static boolean falseIfNull( Boolean result )
    {
    return result == null ? false : result;
    }

  public static boolean falseIfNull( boolean result )
    {
    return result;
    }
  }
