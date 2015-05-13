package mimir.ctables;

import java.io.{StringReader,FileReader}

import net.sf.jsqlparser.parser.{CCJSqlParser}
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

  def db = Database(null);
  def parser = new ExpressionParser((x: String) => boundsSpecModel)
  def expr = parser.expr _

  def bounds = CTAnalyzer.compileForBounds _
  def bounds(x: String): (Expression,Expression) = bounds(expr(x))

  "The Expression Parser" should { 
    "Parse Constants" in {
      expr("1") must be equalTo IntPrimitive(1)
      expr("1.0") must be equalTo FloatPrimitive(1.0)
      expr("'yo'") must be equalTo StringPrimitive("yo")
      expr("bob") must be equalTo Var("bob")
    }
  }

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
  }
}