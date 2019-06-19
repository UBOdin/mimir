package mimir.models

import scala.util._
import org.specs2.mutable._
import mimir.algebra._

object EditDistanceMatchModelSpec extends Specification
{
  
  def train(src: Map[ID,Type], tgt: Map[ID,Type]): Map[ID,(Model,Int)] =
  {
    EditDistanceMatchModel.train(null, ID("TEMP"),
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
            ID("PID") -> TString(),
            ID("EVALUATION") -> TInt(),
            ID("NUM_RATINGS") -> TInt()
          ),Map(
            ID("PID") -> TString(),
            ID("RATING") -> TInt(),
            ID("REVIEW_CT") -> TInt()
          )
        )

      guess(models1(ID("PID"))) must be equalTo("PID")
      guess(models1(ID("RATING"))) must be equalTo("NUM_RATINGS")

      sample(models1(ID("RATING")), 1000) should contain ("NUM_RATINGS", "EVALUATION")
    }

  }


}
