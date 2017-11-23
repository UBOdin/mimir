package mimir.lenses

import java.io._
import mimir._
import mimir.algebra._
import mimir.test._

object TypeInferenceSpec 
  extends SQLTestSpecification("TypeInferenceTest") 
{

  "The Type Inference Lens" should {

    "Be able to create and query type inference lenses" >> {
 
      loadCSV("test/data/CPUSpeed.csv", typeInference = true, detectHeaders = true)

      val baseTypes = db.typechecker.schemaOf(db.table("CPUSPEED_RAW")).toMap
      baseTypes.keys must contain(eachOf("COLUMN_7", "COLUMN_1", "COLUMN_2"))
      baseTypes must contain("COLUMN_7" -> TString())
      baseTypes must contain("COLUMN_1" -> TString())
      baseTypes must contain("COLUMN_2" -> TString())


      val lensTypes = db.typechecker.schemaOf(db.table("CPUSPEED")).toMap
      lensTypes.keys must contain(eachOf("CORES", "FAMILY", "TECH_MICRON"))
      lensTypes must contain("CORES" -> TInt())
      lensTypes must contain("FAMILY" -> TString())
      lensTypes must contain("TECH_MICRON" -> TFloat())

    }

    "Detect Timestamps Correctly" >> {

      Type.matches(TTimestamp(), "2014-06-15 08:23:19") must beTrue
      Type.matches(TTimestamp(), "2013-10-07 08:23:19.120") must beTrue


      loadCSV("test/data/DetectSeriesTest1.csv", typeInference = true, detectHeaders = true)

      val sch = 
        db.typechecker.schemaOf(
          db.table("DETECTSERIESTEST1")
        ).toMap

      sch must contain ("TRAN_TS" -> TTimestamp())

    }

  }


}
