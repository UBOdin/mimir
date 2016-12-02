package mimir.models

import org.specs2.mutable._
import mimir.algebra._
import mimir.algebra.Type._

object TypeInferenceModelSpec extends Specification
{

  def train(elems: List[String]): Model = 
  {
    val model = new TypeInferenceModel("TEST_MODEL", "TEST_COLUMN", 0.5)
    elems.foreach( model.learn(_) )
    return model
  }

  def guess(elems: List[String]): Type.T =
  {
    train(elems).
      bestGuess(0, List[PrimitiveValue]()) match {
        case TypePrimitive(t) => t
      }
  }


  "The Type Inference Model" should {

    "Recognize Integers" >> {

      guess(List("1", "2", "3", "500", "29", "50")) must be equalTo(TInt)

    }

  }
}