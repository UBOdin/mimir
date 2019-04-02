package mimir.algebra

import java.io.File
import org.specs2.mutable._
import org.specs2.specification._
import mimir.parser._
import mimir.algebra._
import mimir.algebra.function.FunctionRegistry
import mimir.optimizer._
import mimir.optimizer.expression._
import mimir.test._

object DateSpec 
  extends SQLTestSpecification("Dates") 
  with BeforeAll
{

  def beforeAll() {
    loadCSV("test/data/DetectSeriesTest2.csv")
  }

  "Dates on SQLite" should {

    "Not be messed up by order-by" >> {

      val noOrderBy = 
      db.query(db.sqlToRA(MimirSQL.Select("SELECT DOB FROM DetectSeriesTest2 WHERE Rank = 1;"))) { _.tuples }
      val withOrderBy = 
        db.query(db.sqlToRA(MimirSQL.Select("SELECT DOB FROM DetectSeriesTest2 WHERE Rank = 1 ORDER BY DOB;"))) { _.tuples }

      withOrderBy must be equalTo(noOrderBy)
    }

  }
}

