package mimir.algebra;

import java.io.{StringReader,FileReader}

import mimir.parser.{MimirJSqlParser}
import org.specs2.mutable._

import mimir._
import mimir.parser._
import mimir.algebra._
import mimir.sql._
import mimir.ctables._

object ParserSpecs extends Specification {
  val schema = Map[String,Map[String,Type]](
    ("R", Map( 
      ("A", TInt()),
      ("B", TInt()),
      ("C", TInt())
    ))
  )
  def model(x:String) = mimir.models.NoOpModel(x, Type.TInt, "TEST")
  def parser = new OperatorParser((x: String) => model(x), schema.get(_).get.toList)
  def expr = parser.expr _
  def oper = parser.operator _
  def sch(x: String) = Table(x, schema.get(x).get.toList, List())

  "The Expression Parser" should { 
    "Parse Constants" in {
      expr("1") must be equalTo IntPrimitive(1)
      expr("1.0") must be equalTo FloatPrimitive(1.0)
      expr("'yo'") must be equalTo StringPrimitive("yo")
      expr("bob") must be equalTo Var("bob")
    }
    "Parse Parenthesization" in {
      expr("(1)") must be equalTo IntPrimitive(1)
    }
    "Parse Conjunctions" in {
      expr("(A = B) & (B = C)") must be equalTo
        Arithmetic(Arith.And,
          Comparison(Cmp.Eq, Var("A"), Var("B")),
          Comparison(Cmp.Eq, Var("B"), Var("C"))
        )
    }
    "Handle VGTerms without parameters" in {
      expr("{{ TR1INFER_0[] }}") must be equalTo
        VGTerm( model("TR1INFER"), 0, List())
    }
    "Handle fields nested in VGTerms" in {
      expr("{{ TEST_0[ 1 ] }}") must be equalTo 
        VGTerm( model("TEST"), 0, List(IntPrimitive(1)))
    }
    "Handle recursively nested VGTerms" in {
      expr("{{ TEST_0[ {{ TEST_1 }} ] }}") must be equalTo
        VGTerm( model("TEST"), 0, List(
          VGTerm( model("TEST"), 1, List())
        ))
    }
    "Handle recursively nested VGTerms mixed with others" in {
      expr("{{ TR1CAST_0[ROWID, {{ TR1INFER_0[] }}] }}") must be equalTo
        VGTerm( model("TR1CAST"), 0, List(
          RowIdVar(),
          VGTerm( model("TR1INFER"), 0, List())
        ))
    }
  }

  "The Operator Parser" should {
    "Parse Relations" in {
      oper("R") must be equalTo sch("R")
    }
    "Parse Projections" in {
      oper(
        "PROJECT[A <= A, B <= B, C <= C](R)"
      ) must be equalTo 
        Project(List(
          ProjectArg("A", expr("A")),
          ProjectArg("B", expr("B")),
          ProjectArg("C", expr("C"))
        ), sch("R"))
    }
    "Parse Selections" in {
      oper(
        "SELECT[A = B](R)"
      ) must be equalTo
        Select(expr("A = B"), sch("R"))
    }
    "Cope with uncertainty" in {
      oper("""
        PROJECT[A <= A, E <= D](
          SELECT[(A = D) & (A = B)](
            PROJECT[A <= R_A, B <= R_B, D <= {{ test_0 }}](R)
          )
        )
      """) must be equalTo
        Project(List(
            ProjectArg("A", expr("A")),
            ProjectArg("E", expr("D"))
          ), 
          Select(expr("(A = D) & (A = B)"),
            Project(List(
                ProjectArg("A", expr("R_A")),
                ProjectArg("B", expr("R_B")),
                ProjectArg("D", expr("{{ test_0 }}"))
              ), 
              sch("R")
            )
          )
        )
    }
  }

}