package mimir.provenance;

import org.specs2.specification._
import org.specs2.mutable._
import java.io.File

import mimir.test._

object ProvenanceRegressions 
  extends SQLTestSpecification("ProvenanceRegressions")
  with BeforeAll
{

  def beforeAll
  {
    loadCSV(targetTable = "R", sourceFile = "test/r_test/r.csv")
    loadCSV(targetTable = "S", sourceFile = "test/r_test/s.csv")
  }

  "Multiple Rowid Columns" should {

    "Exist in joins" >>
      {
        query("SELECT ROWID() FROM R, S WHERE R.B = S.B") {
          result => 
            val row = result.next
            row(0).asString must contain('|')
        }
      }
    "Exist in unions" >>
      {
        query("""
          SELECT ROWID() FROM (
            SELECT B, D FROM S 
              UNION ALL 
            SELECT B, C AS D FROM R
          ) X
        """) {
          result => 
            val row = result.next
            row(0).asString must contain('|')
        }
      }

  }
}