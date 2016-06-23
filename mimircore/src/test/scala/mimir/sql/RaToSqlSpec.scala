package mimir.sql;

import mimir.parser.{MimirJSqlParser}
import org.specs2.mutable._

import mimir._
import mimir.parser._
import mimir.algebra._
import mimir.sql._

object RaToSqlSpec extends Specification  {

  val schema = Map[String,List[(String,Type.T)]](
    ("R", List( 
      ("A", Type.TInt), 
      ("B", Type.TInt)
    ))
  )
  val db = new Database("TestDB", new NullBackend(schema))

  def parser = new OperatorParser((x: String) => null, schema.get(_).get)
  def expr = parser.expr _
  def oper = parser.operator _
  def convert(x: String) = db.convert(oper(x)).toString

  "The RA to SQL converter" should {

    "Produce Flat Queries for Tables" >> {
      convert("R(A, B)") must be equalTo 
        "SELECT R.A AS A, R.B AS B FROM R"
    }

    "Produce Flat Queries for Tables with Aliased Variables" >> {
      convert("R(P, Q)") must be equalTo 
        "SELECT R.A AS P, R.B AS Q FROM R AS R"
    }

    "Produce Flat Queries for Projections" >> {
      convert("PROJECT[Z <= A+B](R(A, B))") must be equalTo 
        "SELECT R.A + R.B AS Z FROM R AS R"
    }

    "Produce Flat Queries for Projections with Aliased Variables" >> {
      convert("PROJECT[Z <= P+Q](R(P, Q))") must be equalTo 
        "SELECT R.A + R.B AS Z FROM R AS R"
    }

    "Produce Flat Queries for Project-Selections" >> {
      convert("PROJECT[Z <= A+B](SELECT[A > B](R(A, B)))") must be equalTo 
        "SELECT R.A + R.B AS Z FROM R AS R WHERE R.A > R.B"
    }

    "Produce Flat Queries for Project-Selections with Aliased Variables" >> {
      convert("PROJECT[Z <= P+Q](SELECT[P > Q](R(P, Q)))") must be equalTo 
        "SELECT R.A + R.B AS Z FROM R AS R WHERE R.A > R.B"
    }

  }

}