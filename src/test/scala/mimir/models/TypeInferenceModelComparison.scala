package mimir.models

import java.io._
import java.util.Calendar
import org.specs2.specification._

import org.specs2.mutable._
import mimir.algebra._
import mimir.util._
import mimir.test._

object TypeInferenceModelComparison extends SQLTestSpecification("TypeInferenceComparisonTests")
with BeforeAll
{
  sequential
  def beforeAll = 
  {
    update("CREATE TABLE Progressive_update(Category1 string, Category2 string)")
    loadCSV("Progressive_update", new File("test/data/InferenceModelProgressiveSanity.csv"))
  }

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
    model.bestGuess(0, List[PrimitiveValue](IntPrimitive(0)), List()) match {
      case TypePrimitive(t) => t
      case x => throw new RAException(s"Type inference model guessed a non-type primitive: $x")
    }
  }
  
  "The Type Inference Model Sanity Check" should
  {
    "Determine first 1000 values" >> 
    {
      println("Running Original method at time: " + Calendar.getInstance.getTime)
      val model = new TypeInferenceModel("PROGRESSIVE_UPDATE:CATEGORY1",Array("CATEGORY1"), 0.5, 1000, db.table("Progressive_update"))
      model.train(db,table("PROGRESSIVE_UPDATE"))
      println("Original method completion time: " + Calendar.getInstance.getTime + "\n" + "And guessed: " + model.getDomain(0, Seq(IntPrimitive(0)), Seq()))
      guess(model) must be equalTo(TInt())
    }
    
    "Determine the subsequent values" >>
    {
      //var progressiveModelResults = Seq( Seq[Any]() ) FOR PRODUCING DATA FOR GRAPHS
      var testerSeq = Seq[String]()
      val model = new TypeInferenceModel("PROGRESSIVE_UPDATE:CATEGORY1",Array("CATEGORY1"),0.5,1000,db.table("Progressive_update"))
      db.models.persist(model)
      while(model.isCompleted() == false){
        //progressiveModelResults=  progressiveModelResults:+Seq(model.getNextSample(), model.getDomain(0, Seq(IntPrimitive(0L)), Seq()),Calendar.getInstance.getTime)
        testerSeq = testerSeq:+"String"
        Thread.sleep(3000)
      }
      //for(item <- progressiveModelResults) println(item)
      guess(model) must be equalTo(TFloat())
    }
  }
  
  
}