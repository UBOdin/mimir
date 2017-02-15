package mimir.lenses

import java.io._
import org.specs2.specification._

import mimir.algebra._
import mimir.util._
import mimir.ctables.{VGTerm}
import mimir.optimizer.{ResolveViews,InlineVGTerms,InlineProjections}
import mimir.test._
import mimir.models._

object KeyRepairSpec 
  extends SQLTestSpecification("KeyRepair") 
  with BeforeAll 
{

  def beforeAll = 
  {
    update("CREATE TABLE R(A int, B int, C int)")
    loadCSV("R", new File("test/r_test/r.csv"))
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
      """).mapRows { row => 
        row(0).asLong.toInt -> (
          row(1).asLong.toInt, 
          row(2).asLong.toInt, 
          row.deterministicCol(0),
          row.deterministicCol(1),
          row.deterministicCol(2),
          row.deterministicRow()
        )
      }.toMap[Int, (Int,Int, Boolean, Boolean, Boolean, Boolean)]

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

  }

}

