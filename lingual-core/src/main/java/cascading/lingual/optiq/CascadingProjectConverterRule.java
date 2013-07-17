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

import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.Convention;

import static cascading.lingual.optiq.Cascading.CONVENTION;

/**
 *
 */
class CascadingProjectConverterRule extends ConverterRule
  {
  public static final CascadingProjectConverterRule INSTANCE = new CascadingProjectConverterRule();

  public CascadingProjectConverterRule()
    {
    super( ProjectRel.class, Convention.NONE, CONVENTION, "CascadingProjectConverterRule" );
    }

  @Override
  public RelNode convert( RelNode rel )
    {
    // stolen from JavaRules.EnumerableAggregateRule
    ProjectRel project = (ProjectRel) rel;

    RelNode convertedChild = convert( project.getChild(), project.getTraitSet().replace( CONVENTION ) );

    if( convertedChild == null )
      return null; // We can't convert the child, so we can't convert rel.

//    return new CascadingProjectRel( rel.getCluster(), rel.getTraitSet(), convertedChild, project.getGroupSet(), project.getAggCallList() );

    return new CascadingProjectRel( rel.getCluster(),
      rel.getTraitSet(),
      convertedChild,
      project.getProjects(),
      project.getRowType(),
      project.getFlags(),
      project.getCollationList() );
    }
  }
