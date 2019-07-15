package mimir.adaptive

import java.io._
import mimir._
import mimir.algebra._
import mimir.test._
import mimir.data._

object TypeInferenceAdaptiveSpec 
  extends SQLTestSpecification("TypeInferenceAdaptiveSpec") 
{

  "Type Inference" should {

    "Be able to create and query type inference adaptive schemas" >> {
 
      db.loader.loadTable(
        "test/data/CPUSpeed.csv", 
        targetTable = Some(ID("CPUSPEED")), 
        format = FileFormat.CSV,
        inferTypes = Some(false)
      )

      db.adaptiveSchemas.create( 
        ID("CPUSPEED_TI"), 
        ID("TYPE_INFERENCE"), 
        db.table("CPUSPEED"), 
        Seq(),
        "CPUSPEED"
      )
      
      val baseTypes = db.typechecker.schemaOf(db.table("CPUSPEED_RAW")).toMap
      baseTypes must contain(ID("_c7") -> TString())
      baseTypes must contain(ID("_c1") -> TString())
      baseTypes must contain(ID("_c2") -> TString())


      val lensTypes = db.typechecker.schemaOf( db.adaptiveSchemas.viewFor(ID("CPUSPEED_TI"), ID("DATA")).get).toMap
      lensTypes must contain(ID("CORES") -> TInt())
      lensTypes must contain(ID("FAMILY") -> TString())
      lensTypes must contain(ID("TECH_MICRON") -> TFloat())

    }

    

  }


}
