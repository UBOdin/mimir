package mimir.lenses

import java.io._
import org.specs2.specification._

import mimir.algebra._
import mimir.util._
import mimir.exec.DefaultOutputFormat
import mimir.ctables.InlineVGTerms
import mimir.optimizer.operator.InlineProjections
import mimir.test._
import mimir.models._
import mimir.parser.MimirSQL

object RepairKeySpec 
  extends SQLTestSpecification("KeyRepair", Map("cleanup" -> "YES")) 
  with BeforeAll 
{

  sequential

  def beforeAll = 
  {
    loadCSV(
      targetTable = "R", 
      sourceFile = "test/r_test/r.csv",
      targetSchema = Seq("A", "B", "C"),
      detectHeaders = false
    )
    loadCSV(
      targetTable = "U", 
      sourceFile = "test/r_test/u.csv",
      targetSchema = Seq("A", "B", "C"),
      detectHeaders = false
    )
    loadCSV(
      targetTable = "FD_DAG", 
      sourceFile = "test/repair_key/fd_dag.csv"
    )
    loadCSV(
      targetTable = "twitter100Cols10kRowsWithScore", 
      sourceFile = "test/r_test/twitter100Cols10kRowsWithScore.csv"
    )
    loadCSV(
      targetTable = "cureSourceWithScore", 
      sourceFile = "test/r_test/cureSourceWithScore.csv"
    )
  }

  "The Key Repair Lens" should {

    "Make Sane Choices" >> {
      update("""
        CREATE LENS R_UNIQUE_A 
          AS SELECT * FROM R
        WITH KEY_REPAIR(A)
      """);

      val result = query("""
        SELECT A, B, C FROM R_UNIQUE_A
      """){ _.map { row => 
        row(ID("A")).asInt -> (
          row(ID("B")).asInt, 
          row(ID("C")).asInt, 
          row.isColDeterministic(ID("A")),
          row.isColDeterministic(ID("B")),
          row.isColDeterministic(ID("C")),
          row.isDeterministic()
        )
      }.toMap[Int, (Int ,Int, Boolean, Boolean, Boolean, Boolean)] }

      result.keys must contain(eachOf(1, 2, 4))
      result must have size(3)

      // The input is deterministic, so the key column "A" should also be deterministic
      result(1)._3 must be equalTo true
      // The input is deterministic, so the row itself is deterministic
      result(1)._6 must be equalTo true
      // There are a number of possibilities for <A:1> on both columns B and C
      result(1)._4 must be equalTo false
      result(1)._5 must be equalTo false

      // The chosen values for <A:1> in columns B and C are arbitrary, but selected
      // from a finite set of possibilities based on what's in R.
      result(1)._1 must be oneOf(2, 3, 4)
      result(1)._2 must be oneOf(1, 2, 3)

      // There is only one populated value for <A:2>[B], the other is null
      result(2)._1 must be equalTo 2
      result(2)._4 must be equalTo true

      // There are two populated values for <A:2>[C], but they're both identical
      result(2)._2 must be equalTo 1
      result(2)._5 must be equalTo true
    }

    "Work with Scores" >> {

      println("For Twitter")

      update("""
        CREATE LENS U_UNIQUE_B
          AS SELECT * FROM U
        WITH KEY_REPAIR(B, SCORE_BY(C))
      """);

      val result = query("""
        SELECT B, A FROM U_UNIQUE_B
      """){ _.map { row => 
        row(ID("B")).asInt -> (
          row(ID("A")).asInt, 
          row.isColDeterministic(ID("B")),
          row.isColDeterministic(ID("A")),
          row.isDeterministic()
        )
      }.toMap[Int, (Int, Boolean, Boolean, Boolean)] }

      result.keys must contain(eachOf(2, 3, 4))
      result(2)._1 must be equalTo(5)
    }

    "Work with the TI lens" >> {
      update("""
        CREATE LENS SCH_REPAIRED
          AS SELECT * FROM FD_DAG
        WITH KEY_REPAIR(ATTR, SCORE_BY(STRENGTH))
      """);

      query("""
        SELECT ATTR, PARENT FROM SCH_REPAIRED
      """){ DefaultOutputFormat.print(_) }

      val result = query("""
        SELECT ATTR, PARENT FROM SCH_REPAIRED
      """){ _.map { row => 
        row(ID("ATTR")).asInt -> row(ID("PARENT")).asInt
      }.toMap[Int, Int] }

      result.keys must contain(eachOf(1, 2, 3, 4))

      result(1) must be equalTo(2)
      result(2) must be equalTo(4)
      result(3) must be equalTo(4)
      result(4) must be equalTo(-1)
    }

    "Update for large data" >> {

      Timer.monitor("CREATE") {
        update(
        """
          CREATE LENS FD_UPDATE
            AS SELECT * FROM twitter100Cols10kRowsWithScore
          WITH KEY_REPAIR(ATTR, SCORE_BY(SCORE))
        """);
      }

      Timer.monitor("QUERY") {
        val result = query("""
          SELECT ATTR, PARENT FROM FD_UPDATE
        """){ _.map { row =>
          row(ID("ATTR")).asInt -> row(ID("PARENT")).asInt
        }.toMap[Int, Int] }
      }

      Timer.monitor("UPDATE") {
        update("""FEEDBACK FD_UPDATE:PARENT 0('1') IS '-1';""")
      }

      println("For CureSource")

      Timer.monitor("CREATE") {
        update(
          """
        CREATE LENS FD_CURE
          AS SELECT * FROM cureSourceWithScore
        WITH KEY_REPAIR(ATTR, SCORE_BY(SCORE))
          """);
      }

      Timer.monitor("QUERY") {
        val result = query("""
          SELECT ATTR, PARENT FROM FD_CURE
        """){ _.map { row =>
            row(ID("ATTR")).asInt -> row(ID("PARENT")).asInt
        }.toMap[Int, Int] }
      }

      Timer.monitor("UPDATE"){
        update("""FEEDBACK FD_CURE:PARENT 0('1') IS '-1';""")
      }

/*
      result.map((out) => {
        println(out)
      })
*/
      true
    }
  }

}

