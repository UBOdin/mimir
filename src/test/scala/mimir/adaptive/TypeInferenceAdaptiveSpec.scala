package mimir.adaptive

import java.io._
import mimir._
import mimir.algebra._
import mimir.test._

object TypeInferenceAdaptiveSpec 
  extends SQLTestSpecification("TypeInferenceAdaptiveSpec") 
{

  "Type Inference" should {

    "Be able to create and query type inference adaptive schemas" >> {
 
      db.loadTable("CPUSPEED", new File("test/data/CPUSpeed.csv"), true, ("CSV", Seq(StringPrimitive(","), BoolPrimitive(false))))

      db.adaptiveSchemas.create( "CPUSPEED_TI", "TYPE_INFERENCE", db.table("CPUSPEED"), Seq())
      
      val baseTypes = db.bestGuessSchema(db.table("CPUSPEED_RAW")).toMap
      baseTypes.keys must contain(eachOf("COLUMN_7", "COLUMN_1", "COLUMN_2"))
      baseTypes must contain("COLUMN_7" -> TString())
      baseTypes must contain("COLUMN_1" -> TString())
      baseTypes must contain("COLUMN_2" -> TString())


      val lensTypes = db.bestGuessSchema( db.adaptiveSchemas.viewFor("CPUSPEED_TI", "CPUSPEED_TI").get).toMap
      lensTypes.keys must contain(eachOf("CORES", "FAMILY", "TECH_MICRON"))
      lensTypes must contain("CORES" -> TInt())
      lensTypes must contain("FAMILY" -> TString())
      lensTypes must contain("TECH_MICRON" -> TFloat())

    }

    

  }


}
