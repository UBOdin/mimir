package mimir.lenses

import java.io._
import org.specs2.specification.core.{Fragment,Fragments}

import mimir.algebra._
import mimir.util._
import mimir.ctables.InlineVGTerms
import mimir.optimizer.operator.InlineProjections
import mimir.test._
import mimir.models._

object LensManagerSpec extends SQLTestSpecification("LensTests") {

  sequential

  "The Lens Manager" should {

    "Be able to create and query missing value lenses" >> {
      update("CREATE TABLE R(A int, B int, C int);")
      loadCSV("R", new File("test/r_test/r.csv"))
      queryOneColumn("SELECT B FROM R"){ _.toSeq should contain(NullPrimitive()) }
      update("CREATE LENS SANER AS SELECT * FROM R WITH MISSING_VALUE('B')")
      queryOneColumn("SELECT B FROM SANER"){ _.toSeq should not contain(NullPrimitive()) }
    }

    "Produce reasonable views" >> {
      db.loadTable("CPUSPEED", new File("test/data/CPUSpeed.csv"))
      val resolved1 = InlineProjections(db.views.resolve(db.table("CPUSPEED")))
      resolved1 must beAnInstanceOf[AdaptiveView]
      resolved1.children.head must beAnInstanceOf[Project]
      val resolved2 = resolved1.children.head.asInstanceOf[Project]
      val coresColumnId = db.table("CPUSPEED").columnNames.indexOf("CORES")
      val coresModel = db.models.get("MIMIR_TI_ATTR_CPUSPEED_TI")

      // Make sure the model name is right.
      // Changes to the way the type inference lens assigns names will need to
      // be reflected above.  That's the only thing that should cause this test
      // to fail.
      coresModel must not be empty

      resolved2.get("CORES") must be equalTo(Some(
        Function("CAST", List(Var("CORES"), TypePrimitive(TInt())))//VGTerm(coresModel.name, coresColumnId, List(), List())))
      ))

      coresModel.reason(coresColumnId, List(), List()) must contain("I guessed that MIMIR_TI_ATTR_CPUSPEED_TI.CORES was of type INT because all of the data fit")

      val coresGuess1 = coresModel.bestGuess(coresColumnId, List(), List())
      coresGuess1 must be equalTo(TypePrimitive(TInt()))

      val coresGuess2 = InlineVGTerms(VGTerm(coresModel.name, coresColumnId, List(), List()), db)
      coresGuess2 must be equalTo(TypePrimitive(TInt()))


    }

    "Clean up after a DROP LENS" >> {

      queryOneColumn(s"""
        SELECT model FROM ${db.models.ownerTable}
        WHERE owner = 'LENS:SANER'
      """){ _.toSeq must not beEmpty }

      val modelNames = db.models.associatedModels("LENS:SANER")
      modelNames must not beEmpty

      update("DROP LENS SANER");
      table("SANER") must throwA[Exception]

      queryOneColumn(s"""
        SELECT model FROM ${db.models.ownerTable}
        WHERE owner = 'LENS:SANER'
      """){ _.toSeq must beEmpty }

      for(model <- modelNames){
        val modelDefn = 
          queryOneColumn(s"""
            SELECT * FROM ${db.models.modelTable} WHERE name = '$model'
          """){ _.toSeq }
        modelDefn must beEmpty;
      }
      ok
    }

  }  

}