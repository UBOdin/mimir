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

object CTPartitionSpec extends Specification {
  
  val boundsSpecModel = IndependentVarsModel("TEST", List(
    UniformDistribution,
    UniformDistribution,
    UniformDistribution,
    UniformDistribution,
    UniformDistribution
  ))
  val schema = Map[String,List[(String,Type)]](
    ("R", List( 
      ("A", TInt()),
      ("B", TInt())
    )),
    ("S", List( 
      ("C", TInt()),
      ("D", TInt())
    ))
  )

  def db = Database(null);
  def parser = new OperatorParser((x: String) => boundsSpecModel, schema(_))
  def expr = parser.expr _
  def oper = parser.operator _

  def partition(x:Operator) = CTPartition.partition(x)
  def partition(x:String) = CTPartition.partition(oper(x))
  def extract(x:Expression) = CTPartition.allCandidateConditions(x)
  def extract(x:String) = CTPartition.allCandidateConditions(expr(x))

  "The Partitioner" should {

    "Handle Base Relations" in {
      partition("R(A, B)") must be equalTo oper("R(A, B)")

      partition("R(A, B // ROWID:rowid <- ROWID)") must be equalTo 
        oper("R(A, B // ROWID:rowid <- ROWID)")
    }

    "Handle Deterministic Projection" in {
      partition("PROJECT[A <= A](R(A, B))") must be equalTo 
        oper("PROJECT[A <= A](R(A, B))")
    }

    "Handle Simple Non-Determinstic Projection" in {
      partition("""
        PROJECT[A <= A, __MIMIR_CONDITION <= {{Q_1[B, ROWID]}}](R(A, B))
      """) must be equalTo oper("""
        PROJECT[A <= A, __MIMIR_CONDITION <= {{Q_1[B, ROWID]}}](R(A, B))
      """)
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
          oper("R(A,B)")
        )
      ) must be equalTo (
        Union(
          Project(List(
              ProjectArg("A", expr("A")),
              ProjectArg(CTables.conditionColumn, expr("{{Q_1[ROWID]}} > 4"))
            ),
            oper("SELECT[A IS NULL](R(A,B))")
          ),
          oper("PROJECT[A <= A](SELECT[(A IS NOT NULL) AND (A > 4)](R(A,B)))")
        )
      )
    }
  }
}