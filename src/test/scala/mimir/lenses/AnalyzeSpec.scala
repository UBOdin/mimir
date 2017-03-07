package mimir.lenses

import java.io._
import org.specs2.specification._
import org.specs2.mutable._
import mimir.test._

object AnalyzeSpec 
  extends SQLTestSpecification("AnalyzeTests")
  with BeforeAll
{

  def beforeAll = 
  {
    update("CREATE TABLE R(A string, B int, C int)")
    loadCSV("R", new File("test/r_test/r.csv"))
    update("CREATE LENS TI AS SELECT * FROM R WITH TYPE_INFERENCE(0.5)")
    update("CREATE LENS MV AS SELECT * FROM TI WITH MISSING_VALUE('B', 'C')")
  }

  "The CTExplainer" should {

    "Explain everything" >> {

      val resultSets = explainEverything("SELECT * FROM MV")
      
      resultSets.map( _.model.name ) must contain(eachOf(
         "MV:WEKA:B", "MV:WEKA:C", "TI"
      ))

      resultSets.map {
        set => (set.model.name -> set.size(db)) 
      }.toMap must contain(eachOf(
        ("MV:WEKA:B" -> 1l),
        ("MV:WEKA:C" -> 1l),
        ("TI" -> 1l)
      ))


    }
  }

}