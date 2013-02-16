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

import java.util.List;

import cascading.lingual.optiq.meta.Branch;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;

/**
 *
 */
public class CascadingProjectRel extends ProjectRelBase implements CascadingRelNode
  {
  /**
   * Creates a Project.
   *
   * @param cluster       Cluster this relational expression belongs to
   * @param traits        traits of this rel
   * @param child         input relational expression
   * @param exps          set of expressions for the input columns
   * @param rowType       output row type
   * @param flags         values as in {@link org.eigenbase.rel.ProjectRelBase.Flags}
   * @param collationList List of sort keys
   */
  protected CascadingProjectRel( RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode[] exps, RelDataType rowType, int flags, final List<RelCollation> collationList )
    {
    super( cluster, traits, child, exps, rowType, flags, collationList );

    assert child.getTraitSet().contains( Cascading.CONVENTION );
    }

  @Override
  public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs )
    {
    return new CascadingProjectRel(
      getCluster(),
      traitSet,
      sole( inputs ),
      exps.clone(),
      rowType,
      flags,
      getCollationList() );
    }

  public Branch visitChild( Stack stack )
    {
    Branch branch = ( (CascadingRelNode) getChild() ).visitChild( stack );

    // todo: skip this project - find rule to collapse this
    if( exps.length == 1 && exps[ 0 ] instanceof RexLiteral )
      {
      RexLiteral rexLiteral = (RexLiteral) exps[ 0 ];
      Class javaType = RelUtil.getJavaType( getCluster(), rexLiteral.getType() );
      Comparable value = rexLiteral.getValue();

      // insert the literal value into the stream, likely upstream to a wildcard aggregation
      Fields fields = RelUtil.getFieldsFor( this ).applyType( 0, javaType );
      Pipe pipe = new Each( branch.current, new Insert( fields, value ), Fields.RESULTS );

      pipe = stack.addDebug( this, pipe );

      return new Branch( pipe, branch );
      }

    Fields currentFields = RelUtil.getTypedFieldsFor( this );
    Fields childFields = RelUtil.getTypedFieldsFor( getChild() );
    Fields narrowChildFields = RelUtil.createTypedFields( getChild(), exps );

    Pipe current = branch.current;

    if( childFields.size() > narrowChildFields.size() )
      current = new Retain( current, narrowChildFields );

    if( !currentFields.equals( narrowChildFields ) )
      current = new Rename( current, narrowChildFields, currentFields );

    current = stack.addDebug( this, current );

    return new Branch( current, branch );
    }
  }
