package mimir.sql;

import mimir.parser.{MimirJSqlParser}
import org.specs2.mutable._
import org.specs2.specification._

import mimir._
import mimir.parser._
import mimir.algebra._
import mimir.sql._
import mimir.test._

object RaToSqlSpec extends SQLTestSpecification("RAToSQL") with BeforeAll {

  def convert(x: String) = db.ra.convert(oper(x)).toString
  def convert(x: Expression) = db.ra.convert(x, List(("R", List("R_A", "R_B")))).toString

  def beforeAll =
  {
    update("CREATE TABLE R(A int, B int)")
  }

  "The RA to SQL converter" should {

    "Produce Flat Queries for Tables" >> {
      convert("R(A, B)") must be equalTo 
        "SELECT * FROM R AS R"
    }

    "Produce Flat Queries for Tables with Aliased Variables" >> {
      convert("R(P, Q)") must be equalTo 
        "SELECT * FROM (SELECT R.A AS P, R.B AS Q FROM R AS R) SUBQ_P"
    }

    "Produce Flat Queries for Projections" >> {
      convert("PROJECT[Z <= A+B](R(A, B))") must be equalTo 
        "SELECT (R.A + R.B) AS Z FROM R AS R"
    }

    "Produce Flat Queries for Projections with Aliased Variables" >> {
      convert("PROJECT[Z <= P+Q](R(P, Q))") must be equalTo 
        "SELECT (SUBQ_P.P + SUBQ_P.Q) AS Z FROM (SELECT R.A AS P, R.B AS Q FROM R AS R) SUBQ_P"
    }

    "Produce Flat Queries for Project-Selections" >> {
      convert("PROJECT[Z <= A+B](SELECT[A > B](R(A, B)))") must be equalTo 
        "SELECT (R.A + R.B) AS Z FROM R AS R WHERE (R.A > R.B)"
    }

    "Produce Flat Queries for Project-Selections with Aliased Variables" >> {
      convert("PROJECT[Z <= P+Q](SELECT[P > Q](R(P, Q)))") must be equalTo 
        "SELECT (SUBQ_P.P + SUBQ_P.Q) AS Z FROM (SELECT R.A AS P, R.B AS Q FROM R AS R) SUBQ_P WHERE (SUBQ_P.P > SUBQ_P.Q)"
    }

  }

  "The Expression Converter" should {

    "Correctly convert conditionals" >> {
      convert(
        Conditional(expr("R_A = 1"), str("A"), str("B"))
      ) must be equalTo
        "CASE WHEN (R.R_A = 1) THEN 'A' ELSE 'B' END"

      convert(
        Conditional(expr("R_A = 1"), str("A"), 
          Conditional(expr("R_A = 2"), str("B"), str("C")))
      ).toString must be equalTo
        "CASE WHEN (R.R_A = 1) THEN 'A' WHEN (R.R_A = 2) THEN 'B' ELSE 'C' END"
      
    }

    "Parenthesize correctly" >> {
      new net.sf.jsqlparser.expression.operators.conditional.AndExpression(
        new net.sf.jsqlparser.schema.Column(
          new  net.sf.jsqlparser.schema.Table(null, null),
          "R_A"
        ),
        new net.sf.jsqlparser.schema.Column(
          new  net.sf.jsqlparser.schema.Table(null, null),
          "R_B"
        )
      ).toString must be equalTo
        "(R_A AND R_B)"

      convert(
        ExpressionUtils.makeOr(
          ExpressionUtils.makeAnd(
            expr("R_A = 1"),
            expr("R_B = 2")
          ),
          ExpressionUtils.makeAnd(
            expr("R_A = 3"),
            expr("R_B = 4")
          )
        )
      ).toString must be equalTo
        "(((R.R_A = 1) AND (R.R_B = 2)) OR ((R.R_A = 3) AND (R.R_B = 4)))"
    }
  }

}