package mimir.models

import scala.util._
import org.specs2.mutable._
import mimir.algebra._

object EditDistanceMatchModelSpec extends Specification
{
  
  def train(src: Map[String,Type], tgt: Map[String,Type]): Map[String,(Model,Int)] =
  {
    EditDistanceMatchModel.train(null, "TEMP",
      Right(src.toList), Right(tgt.toList)
    )
  }

  def guess(model:(Model,Int)): String = 
    model._1.bestGuess(model._2, List(), List()).asString
  def sample(model:(Model,Int), count:Int): List[String] = {
    (0 to count).map( i => 
      model._1.sample(model._2, new Random(), List(), List()).asString
    ).toList
  }

  "The Edit Distance Match Model" should {

    "Make reasonable guesses" >> {

      val models1 = 
        train(Map(
            "PID" -> TString(),
            "EVALUATION" -> TInt(),
            "NUM_RATINGS" -> TInt()
          ),Map(
            "PID" -> TString(),
            "RATING" -> TInt(),
            "REVIEW_CT" -> TInt()
          )
        )

      guess(models1("PID")) must be equalTo("PID")
      guess(models1("RATING")) must be equalTo("NUM_RATINGS")

      sample(models1("RATING"), 1000) should contain ("NUM_RATINGS", "EVALUATION")
    }

  }


}
