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

      db.lenses.create( 
        ID("CPUSPEED_TI"), 
        ID("GUESS_TYPES"), 
        db.table("CPUSPEED"), 
        friendlyName = Some("CPUSPEED")
      )
      
      val baseTypes = db.typechecker.schemaOf(db.table(LoadedTables.SCHEMA, ID("CPUSPEED"))).toMap
      baseTypes must contain(ID("_c7") -> TString())
      baseTypes must contain(ID("_c1") -> TString())
      baseTypes must contain(ID("_c2") -> TString())


      val lensTypes = db.typechecker.schemaOf( db.lenses.view(ID("CPUSPEED_TI")) ).toMap
      lensTypes must contain(ID("CORES") -> TInt())
      lensTypes must contain(ID("FAMILY") -> TString())
      lensTypes must contain(ID("TECH_MICRON") -> TFloat())

    }

    

  }


}
