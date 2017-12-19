package mimir.models

import java.io._
import org.specs2.specification._

import org.specs2.mutable._
import mimir.algebra._
import mimir.util._
import mimir.test._

object TypeInferenceModelComparison extends SQLTestSpecification("TypeInferenceComparisonTests")
with BeforeAll
{
 sequential
 def BeforeAll = {}
 
   def train(elems: List[String]): TypeInferenceModel = 
  {
    val model = new TypeInferenceModel("TEST_MODEL", Array("TEST_COLUMN"), 0.5, 1000, db.table("Dummy_data"))
    elems.foreach( model.learn(0, _) )
    return model
  }

  def guess(elems: List[String]): Type =
    guess(train(elems))

  def guess(model: Model): Type =
  {
    model.bestGuess(0, List[PrimitiveValue](), List()) match {
      case TypePrimitive(t) => t
      case x => throw new RAException(s"Type inference model guessed a non-type primitive: $x")
    }
  }
  
  
}