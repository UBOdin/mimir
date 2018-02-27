package mimir.lenses

import java.io._
import org.specs2.specification._
import org.specs2.mutable._

import mimir.algebra._
import mimir.test._
import mimir.models.FeedbackSource
import mimir.models.FeedbackSourceIdentifier
import mimir.statistics.FeedbackStats

object FeedbackSpec 
  extends SQLTestSpecification("FeedbackTests")
  with BeforeAll
{

  def beforeAll = 
  {
    update("CREATE TABLE R(A string, B int, C int)")
    loadCSV("R", new File("test/r_test/r.csv"))
    update("CREATE ADAPTIVE SCHEMA MATCH AS SELECT * FROM R WITH SCHEMA_MATCHING('B int', 'CX int')")
    db.adaptiveSchemas.create("R_TI", "TYPE_INFERENCE", db.table("R"), Seq(FloatPrimitive(.5))) 
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
      val model = db.models.get("MIMIR_TI_ATTR_R_TI")

      // Base assumptions.  These may change, but the feedback tests 
      // below should be updated accordingly
      model.bestGuess(0, List(IntPrimitive(0)), List()) must be equalTo(TypePrimitive(TInt()))
      db.typechecker.schemaOf(db.adaptiveSchemas.viewFor("R_TI", "DATA").get).
        find(_._1.equals("A")).get._2 must be equalTo(TInt())

      model.feedback(0, List(IntPrimitive(0)), TypePrimitive(TFloat()))
      model.bestGuess(0, List(IntPrimitive(0)), List()) must be equalTo(TypePrimitive(TFloat()))
      db.typechecker.schemaOf(db.adaptiveSchemas.viewFor("R_TI", "DATA").get).
        find(_._1.equals("A")).get._2 must be equalTo(TFloat())
    }
  }

  "The SparkML Model" should {

    "Support direct feedback" >> {
      val model = db.models.get("MV:SPARKML:B")
      val nullRow = querySingleton("SELECT ROWID() FROM R WHERE B IS NULL")

      model.bestGuess(0, List(nullRow), List()) must not be equalTo(50)
      model.feedback(0, List(nullRow), IntPrimitive(50))
      model.bestGuess(0, List(nullRow), List()).asLong must be equalTo(50)

    }

    "Support SQL feedback" >> {
      val model = db.models.get("MV:SPARKML:C")
      val nullRow = querySingleton("SELECT ROWID() FROM R WHERE C IS NULL")

      val originalGuess = model.bestGuess(0, List(nullRow), List()).asLong
      querySingleton(s"""
        SELECT C FROM MV WHERE ROWID() = ROWID($nullRow)
      """) must be equalTo(IntPrimitive(originalGuess))
      originalGuess must not be equalTo(800)

      update(s"FEEDBACK MV:SPARKML:C 0 ($nullRow) IS 800")
      querySingleton(s"""
        SELECT C FROM MV WHERE ROWID() = ROWID($nullRow)
      """) must be equalTo(IntPrimitive(800))



    }
  }
  
  "Feedback Stats" should {
    "Compute feedback source confidence" >> {
      val model = db.models.get("MV:SPARKML:B")
      val nullRow = querySingleton("SELECT ROWID() FROM R WHERE B IS NULL")
      val feedbackConfidence = ((for(i <- 0 to 4) yield {
        val fbSource = FeedbackSourceIdentifier(i.toString(), s"source$i")
        FeedbackSource.setSource(fbSource)
        model.feedback(0, List(nullRow), IntPrimitive(50+i))
        FeedbackSource.setSource(FeedbackSourceIdentifier())
        (fbSource -> {if((50+i) == 50) 1.0 else 0.0})
      }) :+ (FeedbackSourceIdentifier("","truth") -> 1.0)).toSet
      //set ground truth  
      model.feedback(0, List(nullRow), IntPrimitive(50))
      val fbs = new FeedbackStats(db)
      fbs.calcConfidence()
      fbs.fbConfidence.toSet must be equalTo(feedbackConfidence)
    }
  }
}