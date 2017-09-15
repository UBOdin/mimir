package mimir.ctables

import java.io._
import org.specs2.specification._
import org.specs2.mutable._
import mimir.algebra._
import mimir.test._

object CTExplainerSpec 
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
         "MV:SPARK:B", "MV:SPARK:C", "TI"
      ))

      resultSets.map {
        set => (set.model.name -> set.size(db)) 
      }.toMap must contain(eachOf(
        ("MV:SPARK:B" -> 1l),
        ("MV:SPARK:C" -> 1l),
        ("TI" -> 1l)
      ))

      resultSets.map { 
        set => (set.model.name -> set.allArgs(db).map(_._1.toList).toList)
      }.toMap must contain(eachOf(
        ("MV:SPARK:B" -> List(List[PrimitiveValue](RowIdPrimitive("2")))),
        ("MV:SPARK:C" -> List(List[PrimitiveValue](RowIdPrimitive("3")))),
        ("TI" -> List(List()))
      ))
    }

    "Explain individual cells" >> {
      val reasons = 
        explainCell("""
          SELECT * FROM MV
        """, "2", "B"
        ).reasons.map(_.reason).mkString("\n")
      reasons must contain("I used a classifier to guess that MV.B")
    }

    "Explain rows" >> {
      val reasons = 
        explainRow("""
          SELECT * FROM MV
          WHERE C > 1
        """, "3"
        ).reasons.map(_.reason).mkString("\n")
      reasons must contain("I used a classifier to guess that MV.C")

    }

  }

}