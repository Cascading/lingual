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

package cascading.lingual.optiq;

import java.util.ArrayList;
import java.util.List;

import cascading.lingual.optiq.meta.Branch;
import cascading.operation.Filter;
import cascading.operation.expression.ScriptFilter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import net.hydromatic.linq4j.expressions.BlockExpression;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.expressions.Statement;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.rules.java.RexToLixTranslator;
import org.eigenbase.rel.CalcRelBase;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexProgram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public Branch visitChild( Stack stack )
    {
    JavaTypeFactory typeFactory = (JavaTypeFactory) getCluster().getTypeFactory();

    RelDataType inputRowType = getChild().getRowType();
    Fields fields = RelUtil.createFields( getChild(), getProgram().getExprList() );

    List<Expression> parameters = new ArrayList<Expression>();

    for( int i = 0; i < fields.size(); i++ )
      parameters.add( Expressions.parameter( fields.getType( i ), fields.get( i ).toString() ) );

    Expressions.FluentList<Statement> statements = Expressions.<Statement>list();
    Expression condition = RexToLixTranslator.translateCondition(
      parameters,
      program,
      typeFactory,
      statements );

    Branch branch = ( (CascadingRelNode) getChild() ).visitChild( stack );

    List<String> fieldNameList = RelOptUtil.getFieldNameList( inputRowType );
    String[] fieldNames = fieldNameList.toArray( new String[ fieldNameList.size() ] );

    Expression nullToFalse = Expressions.call( CascadingCalcRel.class, "falseIfNull", condition );
    Expression not = Expressions.not( nullToFalse ); // matches #isRemove semantics in Filter

    statements.add( Expressions.return_( null, not ) );

    BlockExpression block = Expressions.block( statements );
    String expression = Expressions.toString( block );

    LOG.debug( "calc expression: {}", expression );

    Filter expressionFilter = new ScriptFilter( expression, fieldNames, fields.getTypes() );
    Pipe pipe = new Each( branch.current, fields, expressionFilter );

    // TODO: Each( ..., filter, ... ) should accept an output selector in the future
    if( !program.projectsIdentity( false ) )
      {
      Fields resultFields = RelUtil.getTypedFields( getCluster(), program.getOutputRowType() );
      pipe = new Retain( pipe, resultFields );
      }

    return new Branch( pipe, branch );
    }

  /**
   * called by the above expression
   *
   * @param result
   * @return
   */
  public static boolean falseIfNull( Boolean result )
    {
    return result == null ? false : result;
    }
  }
