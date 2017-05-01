package mimir.lenses

import java.io._
import org.specs2.specification._

import mimir.algebra._
import mimir.util._
import mimir.exec.DefaultOutputFormat
import mimir.ctables.{VGTerm}
import mimir.optimizer.{InlineVGTerms,InlineProjections}
import mimir.test._
import mimir.models._

object KeyRepairSpec 
  extends SQLTestSpecification("KeyRepair", Map("cleanup" -> "YES")) 
  with BeforeAll 
{

  sequential

  def beforeAll = 
  {
    update("CREATE TABLE R(A int, B int, C int)")
    loadCSV("R", new File("test/r_test/r.csv"))
    update("CREATE TABLE U(A int, B int, C int)")
    loadCSV("U", new File("test/r_test/u.csv"))
    loadCSV("FD_DAG", new File("test/repair_key/fd_dag.csv"))
    loadCSV("twitter100Cols10kRowsWithScore", new File("test/r_test/twitter100Cols10kRowsWithScore.csv"))
    loadCSV("cureSourceWithScore", new File("test/r_test/cureSourceWithScore.csv"))
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
        row("A").asInt -> (
          row("B").asInt, 
          row("C").asInt, 
          row.isColDeterministic("A"),
          row.isColDeterministic("B"),
          row.isColDeterministic("C"),
          row.isDeterministic()
        )
      }.toMap[Int, (Int,Int, Boolean, Boolean, Boolean, Boolean)] }

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
        row("B").asInt -> (
          row("A").asInt, 
          row.isColDeterministic("B"),
          row.isColDeterministic("A"),
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
        row("ATTR").asInt -> row("PARENT").asInt
      }.toMap[Int, Int] }

      result.keys must contain(eachOf(1, 2, 3, 4))

      result(1) must be equalTo(2)
      result(2) must be equalTo(4)
      result(3) must be equalTo(4)
      result(4) must be equalTo(-1)
    }

    "Update for large data" >> {

      TimeUtils.monitor("CREATE") {
        update(
        """
          CREATE LENS FD_UPDATE
            AS SELECT * FROM twitter100Cols10kRowsWithScore
          WITH KEY_REPAIR(ATTR, SCORE_BY(SCORE))
        """);
      }

      TimeUtils.monitor("QUERY") {
        val result = query("""
          SELECT ATTR, PARENT FROM FD_UPDATE
        """){ _.map { row =>
          row("ATTR").asInt -> row("PARENT").asInt
        }.toMap[Int, Int] }
      }

      TimeUtils.monitor("UPDATE") {
        update("""FEEDBACK FD_UPDATE:PARENT 0('1') IS '-1';""")
      }

      println("For CureSource")

      TimeUtils.monitor("CREATE") {
        update(
          """
        CREATE LENS FD_CURE
          AS SELECT * FROM cureSourceWithScore
        WITH KEY_REPAIR(ATTR, SCORE_BY(SCORE))
          """);
      }

      TimeUtils.monitor("QUERY") {
        val result = query("""
          SELECT ATTR, PARENT FROM FD_CURE
        """){ _.map { row =>
            row("ATTR").asInt -> row("PARENT").asInt
        }.toMap[Int, Int] }
      }

      TimeUtils.monitor("UPDATE"){
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

  "RepairKey-FastPath" should {
    "Load Customer Account Balances" >> {
      if(PDBench.isDownloaded){
        update("""
          CREATE TABLE CUST_ACCTBAL_WITHDUPS(
            TUPLE_ID int,
            WORLD_ID int,
            VAR_ID int,
            acctbal float
          )
        """)
        LoadCSV.handleLoadTable(db, 
          "CUST_ACCTBAL_WITHDUPS", 
          new File("test/pdbench/cust_c_acctbal.tbl"), 
          Map(
            "HEADER" -> "NO",
            "DELIMITER" -> "|"
          )
        )
        update("""
          CREATE LENS CUST_ACCTBAL_CLASSIC
          AS SELECT TUPLE_ID, acctbal FROM CUST_ACCTBAL_WITHDUPS
          WITH KEY_REPAIR(TUPLE_ID)
        """)
        TimeUtils.monitor("CREATE_FASTPATH") {
          update("""
            CREATE LENS CUST_ACCTBAL_FASTPATH
            AS SELECT TUPLE_ID, acctbal FROM CUST_ACCTBAL_WITHDUPS
            WITH KEY_REPAIR(TUPLE_ID, ENABLE(FAST_PATH))
          """)
        }
        ok
      } else {
        skipped("Skipping FastPath tests (Run `sbt datasets` to download required data)"); ko
      }
    }

    "Create a fast-path cache table" >> {
      if(PDBench.isDownloaded){
        querySingleton("""
          SELECT COUNT(*)
          FROM (
            SELECT TUPLE_ID, COUNT(DISTINCT WORLD_ID) AS CT
            FROM CUST_ACCTBAL_WITHDUPS
            GROUP BY TUPLE_ID
          ) C
          WHERE CT > 1
        """).asLong must be equalTo(1506l)

        query("""
          SELECT TUPLE_ID
          FROM MIMIR_FASTPATH_CUST_ACCTBAL_FASTPATH
          WHERE num_instances > 1
        """){ _.toSeq must not beEmpty }

        querySingleton("""
          SELECT COUNT(*)
          FROM MIMIR_FASTPATH_CUST_ACCTBAL_FASTPATH
          WHERE num_instances > 1
        """).asLong must be equalTo(1506l)

        querySingleton("""
          SELECT COUNT(*)
          FROM MIMIR_FASTPATH_CUST_ACCTBAL_FASTPATH
          WHERE num_instances = 1
        """).asLong must be equalTo(148494l)

        db.backend.resultValue("""
          SELECT COUNT(*) FROM CUST_ACCTBAL_WITHDUPS
          WHERE WORLD_ID = 1
        """).asLong must be equalTo(150000l)
      } else {
        skipped("Skipping FastPath tests (Run `sbt datasets` to download required data)"); ko
      }
    }

    "Produce the same results" >> {
      if(PDBench.isDownloaded){
        val classic = 
          TimeUtils.monitor("QUERY_CLASSIC"){
            query("""
              SELECT TUPLE_ID, ACCTBAL FROM CUST_ACCTBAL_CLASSIC
            """){ _.map { row => (row("TUPLE_ID").asLong, row("ACCTBAL").asDouble) }.toSeq }
          }
        val fastpath =
          TimeUtils.monitor("QUERY_FASTPATH"){
            query("""
              SELECT TUPLE_ID, ACCTBAL FROM CUST_ACCTBAL_FASTPATH
            """){ _.map { row => (row("TUPLE_ID").asLong, row("ACCTBAL").asDouble) }.toSeq }
          }
        classic.size must be equalTo(150000)
        fastpath.size must be equalTo(150000)
      } else {
        skipped("Skipping FastPath tests (Run `sbt datasets` to download required data)"); ko
      }
    }

    "Produce the same results under selection" >> {
      if(PDBench.isDownloaded){
        db.backend.resultValue("""
          SELECT COUNT(*) FROM CUST_ACCTBAL_WITHDUPS
          WHERE WORLD_ID = 1 and acctbal < 0
        """).asLong must be equalTo(13721l)

        TimeUtils.monitor("QUERY_FASTPATH"){
          queryOneColumn("""
            SELECT TUPLE_ID FROM CUST_ACCTBAL_FASTPATH WHERE acctbal < 0
          """){ _.toSeq.size must be between(13721, 13721+579) }
        }  

        TimeUtils.monitor("QUERY_CLASSIC"){
          queryOneColumn("""
            SELECT TUPLE_ID FROM CUST_ACCTBAL_CLASSIC WHERE acctbal < 0
          """){ _.toSeq.size must be between(13721, 13721+579) }
        }
      } else {
        skipped("Skipping FastPath tests (Run `sbt datasets` to download required data)"); ko
      } 
    }
  }

}

