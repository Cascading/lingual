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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.reltype.RelRecordType;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexLocalRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexProgram;
import org.eigenbase.rex.RexProgramBuilder;

/**
 *
 */
class ProgramUtil
  {
  public static boolean hasFunctions( RexProgram program )
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

  public static boolean hasConstants( RexProgram program )
    {
    List<RexLocalRef> projects = program.getProjectList();

    for( RexLocalRef project : projects )
      {
      if( program.isConstant( project ) )
        return true;
      }

    return false;
    }

  public static <K, V> List<K> leftSlice( final List<? extends Map.Entry<K, V>> pairs )
    {
    return new AbstractList<K>()
    {
    public K get( int index )
      {
      return pairs.get( index ).getKey();
      }

    public int size()
      {
      return pairs.size();
      }
    };
    }

  public static boolean isOnlyRename( RexProgram program )
    {
    RelDataType inputProjects = getInputProjectsRowType( program );
    RelDataType outputProjects = getOutputProjectsRowType( program );

    List<RelDataTypeField> inputList = inputProjects.getFieldList();
    List<RelDataTypeField> outputList = outputProjects.getFieldList();

    final int size = inputList.size();
    if( size != outputList.size() )
      return false;

    if( new HashSet<String>( leftSlice( inputList ) ).size() != size )
      return false;

    if( new HashSet<String>( leftSlice( outputList ) ).size() != size )
      return false;

    for( int i = 0; i < size; i++ )
      {
      RelDataTypeField input = inputList.get( i );
      RelDataTypeField output = outputList.get( i );

      if( !input.getName().equals( output.getName() ) )
        return true;
      }

    return false;
    }

  public static boolean isComplex( RexProgram program )
    {
    RelDataType inputProjects = getInputProjectsRowType( program );
    RelDataType outputProjects = getOutputProjectsRowType( program );

    List<RelDataTypeField> inputList = inputProjects.getFieldList();
    List<RelDataTypeField> outputList = outputProjects.getFieldList();

    final int size = inputList.size();

    if( new HashSet<String>( leftSlice( inputList ) ).size() != size )
      return true;

    if( new HashSet<String>( leftSlice( outputList ) ).size() != size )
      return true;

    return false;
    }

  public static boolean isOnlyProjectsNarrow( RexProgram program )
    {
    List<RexLocalRef> projects = program.getProjectList();
    RelDataType inputRowType = program.getInputRowType();
    int fieldCount = inputRowType.getFieldCount();

    for( RexLocalRef project : projects )
      {
      if( project.getIndex() >= fieldCount )
        return false;
      }

    return true;
    }

  public static RelDataType removeIdentity( RexProgram program )
    {
    RelDataType inputProjects = getInputProjectsRowType( program );
    RelDataType outputProjects = getOutputProjectsRowType( program );

    List<RelDataTypeField> fields = new ArrayList<RelDataTypeField>();

    for( int i = 0; i < inputProjects.getFieldCount(); i++ )
      {
      RelDataTypeField inputField = inputProjects.getFields()[ i ];
      RelDataTypeField outputField = outputProjects.getFields()[ i ];

      if( !inputField.getKey().equals( outputField.getKey() ) )
        fields.add( outputField );
      }

    return new RelRecordType( fields );
    }

  public static RelDataType getDuplicatesRowType( RelDataType inputRowType, RelDataType outputRowType )
    {
    Set<String> outputNames = new HashSet<String>();

    for( RelDataTypeField field : outputRowType.getFields() )
      outputNames.add( field.getKey() );

    List<RelDataTypeField> fields = new ArrayList<RelDataTypeField>();

    for( RelDataTypeField typeField : inputRowType.getFields() )
      {
      if( outputNames.contains( typeField.getKey() ) )
        fields.add( typeField );
      }

    return new RelRecordType( fields );
    }

  public static RelDataType getInputProjectsRowType( RexProgram program )
    {
    RelDataType inputRowType = program.getInputRowType();
    int fieldCount = inputRowType.getFieldCount();
    List<RelDataTypeField> fields = new ArrayList<RelDataTypeField>();
    List<RexLocalRef> projectList = program.getProjectList();

    for( RexLocalRef ref : projectList )
      {
      if( program.isConstant( ref ) || ref.getIndex() >= fieldCount )
        continue;

      fields.add( inputRowType.getFields()[ ref.getIndex() ] );
      }

    return new RelRecordType( fields );
    }

  public static RelDataType getOutputProjectsRowType( RexProgram program )
    {
    RelDataType outputRowType = program.getOutputRowType();
    int fieldCount = program.getInputRowType().getFieldCount(); // must use input field count
    List<RelDataTypeField> fields = new ArrayList<RelDataTypeField>();
    List<RexLocalRef> projectList = program.getProjectList();

    for( int i = 0; i < projectList.size(); i++ )
      {
      RexLocalRef ref = projectList.get( i );

      if( program.isConstant( ref ) || ref.getIndex() >= fieldCount )
        continue;

      fields.add( outputRowType.getFields()[ i ] );
      }

    return new RelRecordType( fields );
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

      fields.add( outputRowType.getFields()[ i ] );
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

  public static RexProgram createNarrowProgram( RexProgram program, RexBuilder rexBuilder )
    {
    RelDataType inputRowType = program.getInputRowType();
    RelDataType outputRowType = program.getOutputRowType();

    RexProgramBuilder builder = new RexProgramBuilder( inputRowType, rexBuilder );

    for( int i = 0; i < program.getExprList().size(); i++ )
      {
      RexNode rexNode = program.getExprList().get( i );

      if( rexNode instanceof RexCall )
        builder.addExpr( rexNode );
      }

    for( int i = 0; i < program.getProjectList().size(); i++ )
      {
      RexLocalRef rexLocalRef = program.getProjectList().get( i );

      RexNode rexNode = program.getExprList().get( rexLocalRef.getIndex() );

      if( rexNode instanceof RexCall )
        builder.addProject( rexNode, outputRowType.getFieldList().get( i ).getName() );
      }

    return builder.getProgram();
    }
  }
