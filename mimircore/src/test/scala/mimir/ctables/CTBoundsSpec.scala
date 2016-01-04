package mimir.ctables;

import java.io.{StringReader,FileReader}

import mimir.parser.{MimirJSqlParser}
import org.specs2.mutable._

import mimir._
import mimir.parser._
import mimir.algebra._
import mimir.sql._

object CTBoundsSpec extends Specification {
  
  val boundsSpecModel = JointSingleVarModel(List(
    UniformDistribution,
    UniformDistribution,
    UniformDistribution,
    UniformDistribution,
    UniformDistribution
  ))

  def db = Database("testdb", null);
  def parser = new ExpressionParser((x: String) => boundsSpecModel)
  def expr = parser.expr _

  def bounds = CTBounds.compile _
  def bounds(x: String): (Expression,Expression) = bounds(expr(x))

  "The Bounds Compiler" should {

    "Compile Constants" in {
      bounds("X") must be equalTo (expr("X"), expr("X"))
    }

    "Compile Basic Arithmetic" in {
      bounds("X+Y") must be equalTo (expr("X+Y"), expr("X+Y"))
      bounds("X-Y") must be equalTo (expr("X-Y"), expr("X-Y"))
      bounds("X*Y") must be equalTo (expr("X*Y"), expr("X*Y"))
      bounds("X/Y") must be equalTo (expr("X/Y"), expr("X/Y"))
      bounds("X&Y") must be equalTo (expr("X&Y"), expr("X&Y"))
      bounds("X|Y") must be equalTo (expr("X|Y"), expr("X|Y"))
    }

    "Compile Const Bounds" in {
      bounds("{{ test_0[1,10] }}") must be equalTo (expr("1"), expr("10"))
    }

    "Compile Trivial Arithmetic Bounds" in {
      bounds(
          "{{ test_0[1,10] }} + {{ test_1[-5,20] }}"
      ) must be equalTo (expr("-4"), expr("30"))
      bounds(
          "{{ test_0[1,10] }} - {{ test_1[-5,20] }}"
      ) must be equalTo (expr("-19"), expr("15"))
    }

    "Handle Simple Multiplication" in {
      bounds(
          "1 * {{ test_0[1,10] }}"
      ) must be equalTo (expr("1"), expr("10"))
      bounds(
          "{{ test_0[1,10] }} * 1"
      ) must be equalTo (expr("1"), expr("10"))
      bounds(
          "{{ test_0[1,10] }} * {{ test_1[2,10] }}"
      ) must be equalTo (expr("2"), expr("100"))
    }

    "Handle Expression Multiplication" in {
      bounds(
          "1 * {{ test_0[1,A] }}"
      ) must be equalTo (expr("__LIST_MIN(1, 1*A)"), expr("__LIST_MAX(1, 1*A)"))
      bounds(
          "{{ test_0[1,A] }} * 1"
      ) must be equalTo (expr("__LIST_MIN(1, A*1)"), expr("__LIST_MAX(1, A*1)"))
      bounds(
          "{{ test_0[1,A] }} * {{ test_1[B,10] }}"
      ) must be equalTo (expr("__LIST_MIN(10, 1*B, A*B, A*10)"), expr("__LIST_MAX(10, 1*B, A*B, A*10)"))
    }

    "Handle Simple Division" in {
      bounds(
          "1 / {{ test_0[1,10] }}"
      ) must be equalTo (expr("0.1"), expr("1.0"))
      bounds(
          "{{ test_0[1,10] }} / 2"
      ) must be equalTo (expr("0.5"), expr("5.0"))
    }

    "Handle Comparisons (eq)" in {
      bounds("{{test_0[0,10]}} = 5") must be equalTo (expr("false"), expr("true"))
      bounds("{{test_0[0,10]}} = -10") must be equalTo (expr("false"), expr("false"))
      bounds("{{test_0[0,10]}} = 20") must be equalTo (expr("false"), expr("false"))
      bounds("{{test_0[5,5]}} = 5") must be equalTo (expr("true"), expr("true"))
    }

    "Handle Comparisons (neq)" in {
      bounds("{{test_0[0,10]}} != 8") must be equalTo (expr("false"), expr("true"))
      bounds("{{test_0[0,10]}} != -18") must be equalTo (expr("true"), expr("true"))
      bounds("{{test_0[0,10]}} != 28") must be equalTo (expr("true"), expr("true"))
      bounds("{{test_0[8,8]}} != 8") must be equalTo (expr("false"), expr("false"))
    }

    "Handle Comparisons (lt)" in {
      bounds("{{test_0[1,11]}} < 7") must be equalTo (expr("false"), expr("true"))
      bounds("{{test_0[0,10]}} < 20") must be equalTo (expr("true"), expr("true"))
      bounds("{{test_0[0,10]}} < -10") must be equalTo (expr("false"), expr("false"))
      bounds("{{test_0[7,10]}} < 7") must be equalTo (expr("false"), expr("false"))
    }
    "Handle Comparisons (lte)" in {
      bounds("{{test_0[1,11]}} <= 7") must be equalTo (expr("false"), expr("true"))
      bounds("{{test_0[0,10]}} <= 20") must be equalTo (expr("true"), expr("true"))
      bounds("{{test_0[0,10]}} <= -10") must be equalTo (expr("false"), expr("false"))
      bounds("{{test_0[9,10]}} <= 9") must be equalTo (expr("false"), expr("true"))
    }

    "Handle Comparisons (gt)" in {
      bounds("{{test_0[1,11]}} > 7") must be equalTo (expr("false"), expr("true"))
      bounds("{{test_0[0,10]}} > -20") must be equalTo (expr("true"), expr("true"))
      bounds("{{test_0[0,10]}} > 20") must be equalTo (expr("false"), expr("false"))
      bounds("{{test_0[0,7]}}  > 7") must be equalTo (expr("false"), expr("false"))
    }
    "Handle Comparisons (gte)" in {
      bounds("{{test_0[1,11]}} >= 7") must be equalTo (expr("false"), expr("true"))
      bounds("{{test_0[0,10]}} >= -20") must be equalTo (expr("true"), expr("true"))
      bounds("{{test_0[0,10]}} >= 20") must be equalTo (expr("false"), expr("false"))
      bounds("{{test_0[0,6]}}  >= 6") must be equalTo (expr("false"), expr("true"))
    }

    "Handle Case Statements with Non-Det Values" in {
      bounds(
        "IF 1 = 1 THEN {{ test_0[1,5] }} ELSE {{ test_1[4,10] }} END"
      ) must be equalTo (expr("1"), expr("5"))
      bounds(
        "IF 2 = 1 THEN {{ test_0[1,5] }} ELSE {{ test_1[4,10] }} END"
      ) must be equalTo (expr("4"), expr("10"))
    }
    "Handle Case Statements with Non-Det Conditions" in {
      bounds(
        "IF {{ test_0[0,2] }} <= 1 THEN 10 ELSE 20 END"
      ) must be equalTo (expr("10.0"), expr("20.0"))
    }
    "Handle Case Statements with Everything Being Non-Det" in {
      bounds(
        "IF {{ test_0[0,2] }} <= 1 THEN {{ test_0[1,5] }} ELSE {{ test_1[4,10] }} END"
      ) must be equalTo (expr("1.0"), expr("10.0"))
    }
    "Handle Case Statements with Det Conditions based on VGTerms" in {
      bounds(
        "IF {{ test_0[0,2] }} <= 5 THEN 10 ELSE 20 END"
      ) must be equalTo (expr("10"), expr("10"))
      bounds(
        "IF {{ test_0[10,12] }} <= 5 THEN 10 ELSE 20 END"
      ) must be equalTo (expr("20"), expr("20"))
    }
  }
}