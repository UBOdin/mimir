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
      
      val baseTypes = db.typechecker.schemaOf(db.table("CPUSPEED_RAW")).toMap
      baseTypes.keys must contain(eachOf("_c7", "_c1", "_c2"))
      baseTypes must contain("_c7" -> TString())
      baseTypes must contain("_c1" -> TString())
      baseTypes must contain("_c2" -> TString())


      val lensTypes = db.typechecker.schemaOf( db.adaptiveSchemas.viewFor("CPUSPEED_TI", "DATA").get).toMap
      lensTypes.keys must contain(eachOf("CORES", "FAMILY", "TECH_MICRON"))
      lensTypes must contain("CORES" -> TInt())
      lensTypes must contain("FAMILY" -> TString())
      lensTypes must contain("TECH_MICRON" -> TFloat())

    }

    

  }


}
