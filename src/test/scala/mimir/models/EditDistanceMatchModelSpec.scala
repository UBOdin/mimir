package mimir.models

import scala.util._
import org.specs2.mutable._
import mimir.algebra._

object EditDistanceMatchModelSpec extends Specification
{
  
  def train(src: Map[String,Type.T], tgt: Map[String,Type.T]): Map[String,(Model,Int)] =
  {
    EditDistanceMatchModel.train(null, "TEMP",
      Right(src.toList), Right(tgt.toList)
    )
  }

  def guess(model:(Model,Int)): String = 
    model._1.bestGuess(model._2, List[PrimitiveValue]()).asString
  def sample(model:(Model,Int), count:Int): List[String] = {
    (0 to count).map( i => 
      model._1.sample(model._2, new Random(), List[PrimitiveValue]()).asString
    ).toList
  }

  "The Edit Distance Match Model" should {

    "Make reasonable guesses" >> {

      val models1 = 
        train(Map(
            "PID" -> Type.TString,
            "EVALUATION" -> Type.TInt,
            "NUM_RATINGS" -> Type.TInt
          ),Map(
            "PID" -> Type.TString,
            "RATING" -> Type.TInt,
            "REVIEW_CT" -> Type.TInt
          )
        )

      guess(models1("PID")) must be equalTo("PID")
      guess(models1("RATING")) must be equalTo("NUM_RATINGS")

      sample(models1("RATING"), 1000) should contain ("NUM_RATINGS", "EVALUATION")
    }

  }


}
