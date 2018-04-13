package mimir.sql;

import mimir.parser.{MimirJSqlParser}
import org.specs2.mutable._
import org.specs2.specification._

import mimir._
import mimir.parser._
import mimir.algebra._
import mimir.sql._
import mimir.test._
import java.io.File
import mimir.util.LoadCSV

object RaToSqlSpec extends SQLTestSpecification("RAToSQL") with BeforeAll {

  def table(name: String)(cols: String*) =
  {
    if(cols.isEmpty){
      name match {
        case "R" => Table(name, name, Seq(("A", TInt()), ("B", TInt())), Seq())
      }
    } else {
      Table(name, name, cols.map { (_, TInt()) }, Seq())      
    }
  }

  def expr(x: String) = ExpressionParser.expr(x)
  def convert(x: Operator) = db.ra.convert(x).toString
  def convert(x: Expression) = db.ra.convert(x, List(("R", List("R_A", "R_B")))).toString

  def beforeAll =
  {
    LoadCSV.handleLoadTableRaw(db, "R", Some(Seq(("A",TInt()),("B",TInt()))), new File("test/data/serial_r.csv"),  Map("DELIMITER" -> ",", "mode" -> "DROPMALFORMED", "header" -> "false") )
  }
  sequential
  "The RA to SQL converter" should {
    sequential
  
    "Produce Flat Queries for Tables" >> {
      convert(
        table("R")()
      ) must be equalTo 
        "SELECT * FROM R AS R"
    }

    "Produce Flat Queries for Tables with Aliased Variables" >> {
      convert(
        table("R")("P", "Q")
      ) must be equalTo 
        "SELECT * FROM (SELECT R.A AS P, R.B AS Q FROM R AS R) SUBQ_P"
    }

    "Produce Flat Queries for Projections" >> {
      convert(
        table("R")()
          .mapParsed( ("Z", "A+B") )
      ) must be equalTo 
        "SELECT (R.A + R.B) AS Z FROM R AS R"
    }

    "Produce Flat Queries for Projections with Aliased Variables" >> {
      convert(
        table("R")("P", "Q")
          mapParsed( ("Z", "P+Q") )
      ) must be equalTo 
        "SELECT (SUBQ_P.P + SUBQ_P.Q) AS Z FROM (SELECT R.A AS P, R.B AS Q FROM R AS R) SUBQ_P"
    }

    "Produce Flat Queries for Project-Selections" >> {
      convert(
        table("R")()
          .filterParsed( "A > B" )
          .mapParsed( ("Z", "A+B") )
      ) must be equalTo 
        "SELECT (R.A + R.B) AS Z FROM R AS R WHERE (R.A > R.B)"
    }

    "Produce Flat Queries for Project-Selections with Aliased Variables" >> {
      convert(
        table("R")("P", "Q")
          .filterParsed( "P > Q" )
          mapParsed( ("Z", "P+Q") )
      ) must be equalTo 
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