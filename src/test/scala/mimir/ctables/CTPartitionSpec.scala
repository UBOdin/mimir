package mimir.ctables;

import java.io.{StringReader,FileReader}

import mimir.parser.{MimirJSqlParser}
import org.specs2.mutable._

import mimir._
import mimir.parser._
import mimir.algebra._
import mimir.sql._
import mimir.util._
import mimir.models._
import mimir.test.RASimplify

object CTPartitionSpec 
  extends Specification 
  with RASimplify
{
  
  val schema = Map[String,Seq[(String,Type)]](
    ("R", Seq( 
      ("A", TInt()),
      ("B", TInt())
    )),
    ("S", Seq( 
      ("C", TInt()),
      ("D", TInt())
    ))
  )

  def table(name: String, meta: Seq[(String, Expression, Type)] = Seq()): Operator =
    Table(name, name, schema(name), meta)

  val withRowId = Seq(("ROWID", RowIdVar(), TRowId()))

  def db = Database(null, null);
  def expr = ExpressionParser.expr _

  def partition(x:Operator) = simplify(CTPartition.partition(x))
  def extract(x:Expression) = CTPartition.allCandidateConditions(x)

  "The Partitioner" should {

    "Handle Base Relations" in {
      partition(table("R")) must be equalTo table("R")

      partition(table("R", withRowId)) must be equalTo 
        table("R", withRowId)
    }

    "Handle Deterministic Projection" in {
      partition(
        table("R").
          project("A")
      ) must be equalTo(
        table("R").
          project("A")
      )
    }

    "Handle Simple Non-Determinstic Projection" in {
      partition(
        table("R").
          mapParsed(
            "A" -> "A",
            "__MIMIR_CONDITION" -> "{{Q_1[B, ROWID]}}"
          )
      ) must be equalTo (
        table("R").
          mapParsed(
            "A" -> "A",
            "__MIMIR_CONDITION" -> "{{Q_1[B, ROWID]}}"
          )
      )
    }

    "Extract Conditions Correctly" in {
      extract(
        Conditional(expr("A IS NULL"), expr("{{Q_1[ROWID]}}"), expr("A"))
      ) must be equalTo(
        List(expr("A IS NULL"), expr("A IS NOT NULL"))
      )
    }

    "Handle Conditional Non-Determinstic Projection" in {
      partition(
        Project(List(
            ProjectArg("A", expr("A")),
            ProjectArg(CTables.conditionColumn, 
              Comparison(Cmp.Gt,
                Conditional(expr("A IS NULL"), expr("{{Q_1[ROWID]}}"), expr("A")),
                expr("4")
              )
            )
          ),
          table("R")
        )
      ) must be equalTo (
        Union(
          Project(List(
              ProjectArg("A", expr("A")),
              ProjectArg(CTables.conditionColumn, expr("{{Q_1[ROWID]}} > 4"))
            ),
            table("R").filterParsed("A IS NULL")
          ),
          table("R")
            .filterParsed("A IS NOT NULL")
            .mapParsed(
              "A" -> "A",
              CTables.conditionColumn -> "A > 4"
            )
        )
      )
    }
  }
}