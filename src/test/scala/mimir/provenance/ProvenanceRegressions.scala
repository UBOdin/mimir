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
    loadCSV("R", Seq(("A", "int"), ("B", "int"), ("C", "int")), new File("test/r_test/r.csv"))
    loadCSV("S", Seq(("B", "int"), ("D", "int")), new File("test/r_test/s.csv"))
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