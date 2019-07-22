package mimir.sql;

import org.specs2.mutable._
import org.specs2.specification._

import mimir._
import mimir.parser._
import mimir.algebra._
import mimir.sql._
import mimir.test._
import mimir.data.LoadedTables
import java.io.File
import sparsity.Name
import mimir.data.FileFormat

object RaToSqlSpec extends SQLTestSpecification("RAToSQL") with BeforeAll {

  def table(name: String)(cols: String*) =
  {
    if(cols.isEmpty){
      name match {
        case "R" => Table(ID(name), LoadedTables.SCHEMA, Seq(ID("_c0") -> TInt(), ID("_c1") -> TInt()), Seq())
      }
    } else {
      Table(ID(name), LoadedTables.SCHEMA, cols.map { ID.upper(_) }.map { (_, TInt()) }, Seq())      
    }
  }

  def expr(x: String) = ExpressionParser.expr(x)
  def convert(x: Operator) = db.raToSQL(x).toString
  def convert(x: Expression) = db.raToSQL(x, List((Name("R"), List(Name("R_A"), Name("R_B"))))).toString

  def beforeAll =
  {
    db.loader.linkTable(
      sourceFile = "test/data/serial_r.csv",
      format = FileFormat.CSV,
      targetTable = ID("R"), 
      sparkOptions = Map("DELIMITER" -> ",", "mode" -> "DROPMALFORMED", "header" -> "false") 
    )
  }
  sequential
  "The RA to SQL converter" should {
    sequential
  
    "Produce Flat Queries for Tables" >> {
      convert(
        table("R")()
      ) must be equalTo 
        "SELECT * FROM `R`"
    }

    "Produce Flat Queries for Tables with Aliased Variables" >> {
      convert(
        table("R")("P", "Q")
      ) must be equalTo 
        "SELECT * FROM (SELECT `R`.`_c0` AS `P`, `R`.`_c1` AS `Q` FROM `R`) AS `SUBQ_P`"
    }

    "Produce Flat Queries for Projections" >> {
      convert(
        table("R")()
          .mapParsed( ("Z", "`_c0`+`_c1`") )
      ) must be equalTo 
        "SELECT `R`.`_c0` + `R`.`_c1` AS `Z` FROM `R`"
    }

    "Produce Flat Queries for Projections with Aliased Variables" >> {
      convert(
        table("R")("P", "Q")
          mapParsed( ("Z", "P+Q") )
      ) must be equalTo 
        "SELECT `SUBQ_P`.`P` + `SUBQ_P`.`Q` AS `Z` FROM (SELECT `R`.`_c0` AS `P`, `R`.`_c1` AS `Q` FROM `R`) AS `SUBQ_P`"
    }

    "Produce Flat Queries for Project-Selections" >> {
      convert(
        table("R")()
          .filterParsed( "A > B" )
          .mapParsed( ("Z", "A+B") )
      ) must be equalTo 
        "SELECT `R`.`A` + `R`.`B` AS `Z` FROM `R` WHERE `R`.`A` > `R`.`B`"
    }

    "Produce Flat Queries for Project-Selections with Aliased Variables" >> {
      convert(
        table("R")("P", "Q")
          .filterParsed( "P > Q" )
          mapParsed( ("Z", "P+Q") )
      ) must be equalTo 
        "SELECT `SUBQ_P`.`P` + `SUBQ_P`.`Q` AS `Z` FROM (SELECT `R`.`A` AS `P`, `R`.`B` AS `Q` FROM `R`) AS `SUBQ_P` WHERE `SUBQ_P`.`P` > `SUBQ_P`.`Q`"
    }

  }

  "The Expression Converter" should {

    "Correctly convert conditionals" >> {
      convert(
        Conditional(expr("R_A = 1"), str("A"), str("B"))
      ) must be equalTo
        "CASE WHEN (R.`R_A` = 1) THEN 'A' ELSE 'B' END"

      convert(
        Conditional(expr("R_A = 1"), str("A"), 
          Conditional(expr("R_A = 2"), str("B"), str("C")))
      ).toString must be equalTo
        "CASE WHEN (R.`R_A` = 1) THEN 'A' WHEN (R.`R_A` = 2) THEN 'B' ELSE 'C' END"
      
    }

    "Parenthesize correctly" >> {
      sparsity.expression.Arithmetic(
        sparsity.expression.Column(Name("R_A")),
        sparsity.expression.Arithmetic.And,
        sparsity.expression.Column(Name("R_B"))
      ).toString must be equalTo
        "R_A AND R_B"

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
        "((R.`R_A` = 1) AND (R.`R_B` = 2)) OR ((R.`R_A` = 3) AND (R.`R_B` = 4))"
    }
  }

}