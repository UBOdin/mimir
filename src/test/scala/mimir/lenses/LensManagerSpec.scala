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
      loadCSV(
        targetTable = "R", 
        sourceFile = "test/r_test/r.csv",
        targetSchema = Seq("A", "B", "C")
      )
      queryOneColumn("SELECT B FROM R"){ _.toSeq should contain(NullPrimitive()) }
      update("CREATE LENS SANER AS SELECT * FROM R WITH MISSING_VALUE('B')")
      queryOneColumn("SELECT B FROM SANER"){ _.toSeq should not contain(NullPrimitive()) }
    }

    "Produce reasonable views" >> {
      db.loader.loadTable(targetTable = Some(ID("CPUSPEED")), sourceFile = "test/data/CPUSpeed.csv")
      val resolved1 = InlineProjections(db.views.resolve(db.table("CPUSPEED")))
      resolved1 must beAnInstanceOf[Project]
      val resolved2 = resolved1.asInstanceOf[Project]
      val coresColumnId = db.table("CPUSPEED").columnNames.indexOf(ID("CORES"))
      // val coresModel = db.models.get(ID("MIMIR_TI_ATTR_CPUSPEED_TI"))

      // // Make sure the model name is right.
      // // Changes to the way the type inference lens assigns names will need to
      // // be reflected above.  That's the only thing that should cause this test
      // // to fail.
      // coresModel must not be empty

      // coresModel.reason(0, List(IntPrimitive(coresColumnId)), List()) must contain("I guessed that CPUSPEED.CORES was of type INT because all of the data fit")

      // val coresGuess1 = coresModel.bestGuess(0, List(IntPrimitive(coresColumnId)), List())
      // coresGuess1 must be equalTo(TypePrimitive(TInt()))

      // val coresGuess2 = InlineVGTerms(VGTerm(coresModel.name, 0, List(IntPrimitive(coresColumnId)), List()), db)
      // coresGuess2 must be equalTo(TypePrimitive(TInt()))

      ok

    }

    "Clean up after a DROP LENS" >> {

      // val modelNamesBefore = db.models.associatedModels(ID("LENS:SANER"))
      // modelNamesBefore must not beEmpty

      // update("DROP LENS SANER");
      // table("SANER") must throwA[Exception]

      // val modelNamesAfter = db.models.associatedModels(ID("LENS:SANER"))
      // modelNamesAfter must beEmpty

      ok
    }

  }  

}