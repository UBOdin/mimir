package mimir.models

import java.io._
import org.specs2.specification._

import org.specs2.mutable._
import mimir.algebra._
import mimir.util._
import mimir.test._

object TypeInferenceModelSpec extends SQLTestSpecification("TypeInferenceTests")
with BeforeAll
{
  sequential
  def beforeAll = 
  {
    update("CREATE TABLE Dummy_data(ATTR int, PARENT int, strength float)")
    loadCSV("Dummy_data", new File("test/repair_key/fd_Dag.csv"))
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
    model.bestGuess(0, List[PrimitiveValue](), List()) match {
      case TypePrimitive(t) => t
      case x => throw new RAException(s"Type inference model guessed a non-type primitive: $x")
    }
  }


  "The Type Inference Model" should {
    "Recognize Integers" >> {
      guess(List("1", "2", "3", "500", "29", "50")) must be equalTo(TInt())
    }
    "Recognize Floats" >> {
      guess(List("1.0", "2.0", "3.2", "500.1", "29.9", "50.0000")) must be equalTo(TFloat())
      guess(List("1", "2", "3", "500", "29", "50.0000")) must be equalTo(TFloat())
    }
    "Recognize Dates" >> {
      guess(List("1984-11-05", "1951-03-23", "1815-12-10")) must be equalTo(TDate())
    }
    "Recognize Strings" >> {
      guess(List("Alice", "Bob", "Carol", "Dave")) must be equalTo(TString())
      guess(List("Alice", "Bob", "Carol", "1", "2.0")) must be equalTo(TString())
    }

    "Recognize CPU Cores" >> {
      loadCSV("CPUSPEED", new File("test/data/CPUSpeed.csv"))
      val model = new TypeInferenceModel("CPUSPEED:CORES", Array("CORES"), 0.5, 1000, db.table("CPUSPEED"))
      LoggerUtils.debug(
        //"mimir.models.TypeInferenceModel"
      ){
        model.train(db, table("CPUSPEED"))
        guess(model) must be equalTo(TInt())
      }
    }
    
    "Update itself" >> {
      update("CREATE TABLE Progressive_update(Category1 int, Category2 string, Category3 float)")
      loadCSV("Progressive_update", new File("test/data/InferenceModelProgressive.csv"))
      val model = new TypeInferenceModel("PROGRESSIVE_UPDATE:CATEGORY3",Array("CATEGORY3"),0.5,1000,db.table("Progressive_update"))
      db.models.persist(model)
      //LoggerUtils.debug(){
      model.train(db, table("PROGRESSIVE_UPDATE"))
      Thread.sleep(30000)
      db.models.get("PROGRESSIVE_UPDATE:CATEGORY3").asInstanceOf[ProgressiveUpdate].getNextSample() must be greaterThan(2000) 
      //} 
    }
    
    "Complete updating" >> {
      val model = new TypeInferenceModel("PROGRESSIVE_UPDATE:CATEGORY2", Array("CATEGORY2"),0.5,1000,db.table("Progressive_update"))
      db.models.persist(model)
      //LoggerUtils.debug(){
        model.train(db, table("PROGRESSIVE_UPDATE"))
        Thread.sleep(30000)
      //}
      model.asInstanceOf[ProgressiveUpdate].isCompleted() must be equalTo(true)
    }
    
    "Successfully determine type from large datasets" >> {
     val model = new TypeInferenceModel("PROGRESSIVE_UPDATE:CATEGORY3",Array("CATEGORY3"),0.5,1000,db.table("Progressive_update"))
     db.models.persist(model)
     model.train(db,table("PROGRESSIVE_UPDATE"))
     Thread.sleep(30000)
     guess(model) must be equalTo(TFloat())
    }
    
    
  }
}