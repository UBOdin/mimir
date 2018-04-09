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
    db.loadTable("R", Seq(("A","string"),("B","int"),("C","int")),new File("test/r_test/r.csv"))
    db.adaptiveSchemas.create("R_TI", "TYPE_INFERENCE", db.table("R"), Seq(FloatPrimitive(.5))) 
		db.views.create("TI", db.adaptiveSchemas.viewFor("R_TI", "DATA").get)
    update("CREATE LENS MV AS SELECT * FROM TI WITH MISSING_VALUE('B', 'C')")
  }

  "The CTExplainer" should {

    "Explain everything" >> {

      val resultSets = db.explainer.explainEverything(table("MV"))
      
      resultSets.map( _.model.name ) must contain(eachOf(
         "MV:SPARKML:B", "MV:SPARKML:C", "MIMIR_TI_ATTR_R_TI"
      ))

      resultSets.map {
        set => (set.model.name -> set.size(db)) 
      }.toMap must contain(eachOf(
        ("MV:SPARKML:B" -> 1l),
        ("MV:SPARKML:C" -> 1l),
        ("MIMIR_TI_ATTR_R_TI" -> 1l)
      ))

      resultSets.map { 
        set => (set.model.name -> set.allArgs(db).map(_._1.toList).toList)
      }.toMap must contain(eachOf(
        ("MV:SPARKML:B" -> List(List[PrimitiveValue](RowIdPrimitive("3")))),
        ("MV:SPARKML:C" -> List(List[PrimitiveValue](RowIdPrimitive("4")))),
        ("MIMIR_TI_ATTR_R_TI",List(List(IntPrimitive(0))))
      ))
    }

    "Explain individual cells" >> {
      val reasons = 
        explainCell("""
          SELECT * FROM MV
        """, "3", "B"
        ).reasons.map(_.reason).mkString("\n")
      reasons must contain("I used a classifier to guess that MV.B")
    }

    "Explain rows" >> {
      val reasons = 
        explainRow("""
          SELECT * FROM MV
          WHERE C > 1
        """, "4"
        ).reasons.map(_.reason).mkString("\n")
      reasons must contain("I used a classifier to guess that MV.C")

    }

  }

}