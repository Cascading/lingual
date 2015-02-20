/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import java.math.BigDecimal;
import java.util.Collections;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexLocalRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexProgram;
import org.eigenbase.rex.RexProgramBuilder;
import org.eigenbase.util.NlsString;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

/** Unit test for {@link CalcProjectUtil}. */
public class CalcProjectUtilTest
  {
  @Test
  /** Tests that we can analyze a program with no inputs or outputs. */
  public void testAnalyzeProgram()
    {
    final RelDataTypeFactory typeFactory = new FieldTypeFactory();
    final RelDataType inputType = typeFactory.createStructType( new RelDataTypeFactory.FieldInfoBuilder() );
    final RelDataType outputType = typeFactory.createStructType( new RelDataTypeFactory.FieldInfoBuilder() );
    final RexProgram program = new RexProgram(
      inputType,
      Collections.<RexNode>emptyList(),
      Collections.<RexLocalRef>emptyList(),
      null,
      outputType );
    final ProgramUtil.Analyzed analyze = ProgramUtil.analyze( program );
    assertFalse( analyze.hasConstants );
    assertFalse( analyze.hasFunctions );
    assertNotNull( analyze.permutation );
    assertTrue( analyze.permutation.isIdentity() );
    assertEquals( 0, analyze.permutation.size() );
    assertFalse( analyze.isComplex );
    }

  @Ignore
  @Test
  /** Tests that we can analyze a program with no inputs that outputs two
   * constants. */
  public void testAnalyzeTwoConstantProgram()
    {
    final RelDataTypeFactory typeFactory = new FieldTypeFactory();
    final RelDataType inputType = typeFactory.createStructType( new RelDataTypeFactory.FieldInfoBuilder() );
    final RexBuilder builder = new RexBuilder( typeFactory );
    final RexProgramBuilder programBuilder = new RexProgramBuilder( inputType, builder );
    programBuilder.addProject( builder.makeExactLiteral( BigDecimal.ONE ), "x" );
    programBuilder.addProject( builder.makeCharLiteral( new NlsString( "foo", null, null ) ), "y" );
    final RexProgram program = programBuilder.getProgram();
    final ProgramUtil.Analyzed analyze = ProgramUtil.analyze( program );
    assertTrue( analyze.hasConstants );
    assertFalse( analyze.hasFunctions );
    assertNull( analyze.permutation );
    assertFalse( analyze.isComplex );
    }
  }
