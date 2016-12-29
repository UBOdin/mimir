package mimir.lenses

import java.io._
import org.specs2.specification._
import org.specs2.mutable._
import mimir.test._

object FeedbackSpec 
  extends SQLTestSpecification("LensTests")
  with BeforeAll
{

  sequential

  def beforeAll = 
  {
    update("CREATE TABLE R(A string, B int, C int)")
    loadCSV("R", new File("test/r_test/r.csv"))
    update("CREATE LENS MATCH AS SELECT * FROM R WITH SCHEMA_MATCHING('B int', 'CX int')")
  }

  "The Edit Distance Match Model" should {

    "Support direct feedback" >> {
      val model = db.models.get("MATCH:EDITDISTANCE:B")

      // Base assumptions.  These may change, but the feedback tests 
      // below should be updated accordingly
      model.bestGuess(0, List()) must be equalTo(str("B"))

      model.feedback(0, List(), str("C"))
      model.bestGuess(0, List()) must be equalTo(str("C"))
    }

    "Support SQL Feedback" >> {
      val model = db.models.get("MATCH:EDITDISTANCE:CX")

      // Base assumptions.  These may change, but the feedback tests 
      // below should be updated accordingly
      model.bestGuess(0, List()) must be equalTo(str("C"))

      // Test Model C
      update("FEEDBACK MATCH:EDITDISTANCE:CX 0 IS 'B'")
      model.bestGuess(0, List()) must be equalTo(str("B"))
    }

  }
}