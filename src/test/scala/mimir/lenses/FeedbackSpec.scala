package mimir.lenses

import java.io._
import org.specs2.specification._
import org.specs2.mutable._

import mimir.algebra._
import mimir.test._

object FeedbackSpec 
  extends SQLTestSpecification("FeedbackTests")
  with BeforeAll
{

  def beforeAll = 
  {
    update("CREATE TABLE R(A string, B int, C int)")
    loadCSV("R", new File("test/r_test/r.csv"))
    update("CREATE LENS MATCH AS SELECT * FROM R WITH SCHEMA_MATCHING('B int', 'CX int')")
    update("CREATE LENS TI AS SELECT * FROM R WITH TYPE_INFERENCE(0.5)")
    update("CREATE LENS MV AS SELECT * FROM R WITH MISSING_VALUE('B', 'C')")
  }

  "The Edit Distance Match Model" should {

    "Support direct feedback" >> {
      val model = db.models.get("MATCH:EDITDISTANCE:B")

      // Base assumptions.  These may change, but the feedback tests 
      // below should be updated accordingly
      model.bestGuess(0, List(), List()) must be equalTo(str("B"))

      model.feedback(0, List(), str("C"))
      model.bestGuess(0, List(), List()) must be equalTo(str("C"))
    }

    "Support SQL Feedback" >> {
      val model = db.models.get("MATCH:EDITDISTANCE:CX")

      // Base assumptions.  These may change, but the feedback tests 
      // below should be updated accordingly
      model.bestGuess(0, List(), List()) must be equalTo(str("C"))

      // Test Model C
      update("FEEDBACK MATCH:EDITDISTANCE:CX 0 IS 'B'")
      model.bestGuess(0, List(), List()) must be equalTo(str("B"))
    }

  }

  "The Type Inference Model" should {

    "Support direct feedback" >> {
      val model = db.models.get("TI")

      // Base assumptions.  These may change, but the feedback tests 
      // below should be updated accordingly
      model.bestGuess(0, List(), List()) must be equalTo(TypePrimitive(TInt()))
      db.bestGuessSchema(table("TI")).
        find(_._1.equals("A")).get._2 must be equalTo(TInt())

      model.feedback(0, List(), TypePrimitive(TFloat()))

      model.bestGuess(0, List(), List()) must be equalTo(TypePrimitive(TFloat()))
      db.bestGuessSchema(table("TI")).
        find(_._1.equals("A")).get._2 must be equalTo(TFloat())
    }
  }

  "The Weka Model" should {

    "Support direct feedback" >> {
      val model = db.models.get("MV:SPARK:B")
      val nullRow = querySingleton("SELECT ROWID() FROM R WHERE B IS NULL")

      model.bestGuess(0, List(nullRow), List()) must not be equalTo(50)
      model.feedback(0, List(nullRow), IntPrimitive(50))
      model.bestGuess(0, List(nullRow), List()).asLong must be equalTo(50)

    }

    "Support SQL feedback" >> {
      val model = db.models.get("MV:SPARK:C")
      val nullRow = querySingleton("SELECT ROWID() FROM R WHERE C IS NULL")

      val originalGuess = model.bestGuess(0, List(nullRow), List()).asLong
      querySingleton(s"""
        SELECT C FROM MV WHERE ROWID() = ROWID($nullRow)
      """) must be equalTo(IntPrimitive(originalGuess))
      originalGuess must not be equalTo(800)

      update(s"FEEDBACK MV:SPARK:C 0 ($nullRow) IS 800")
      querySingleton(s"""
        SELECT C FROM MV WHERE ROWID() = ROWID($nullRow)
      """) must be equalTo(IntPrimitive(800))



    }

  }
}