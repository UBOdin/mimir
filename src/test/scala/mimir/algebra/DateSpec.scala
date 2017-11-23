package mimir.algebra

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
    loadCSV("test/data/DetectSeriesTest2.csv", typeInference = true, detectHeaders = true)
  }

  "Dates on SQLite" should {

    "Not be messed up by order-by" >> {

      val noOrderBy = 
        db.query("SELECT DOB FROM DetectSeriesTest2 WHERE Rank = 1;") { _.tuples }
      val withOrderBy = 
        db.query("SELECT DOB FROM DetectSeriesTest2 WHERE Rank = 1 ORDER BY DOB;") { _.tuples }

      withOrderBy must be equalTo(noOrderBy)
    }

  }
}

