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

/**
 * Unit test for {@link CalcProjectUtil}.
 */
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
