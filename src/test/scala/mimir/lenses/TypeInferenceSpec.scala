package mimir.lenses

import java.io._
import mimir._
import mimir.algebra._
import mimir.test._

object TypeInferenceSpec 
  extends SQLTestSpecification("TypeInferenceTest") 
{

  "The Type Inference Lens" should {

    "Be able to create and query type inference adaptive schemas" >> {
 
      db.loadTable("CPUSPEED", new File("test/data/CPUSpeed.csv"))

      val baseTypes = db.typechecker.schemaOf(db.table("CPUSPEED_RAW")).toMap
      baseTypes.keys must contain(eachOf("_c7", "_c1", "_c2"))
      baseTypes must contain("_c7" -> TString())
      baseTypes must contain("_c1" -> TString())
      baseTypes must contain("_c2" -> TString())


      val lensTypes = db.typechecker.schemaOf(db.table("CPUSPEED")).toMap
      lensTypes.keys must contain(eachOf("CORES", "FAMILY", "TECH_MICRON"))
      lensTypes must contain("CORES" -> TInt())
      lensTypes must contain("FAMILY" -> TString())
      lensTypes must contain("TECH_MICRON" -> TFloat())

    }

    "Detect Timestamps Correctly" >> {

      db.types.testForTypes("2014-06-15 08:23:19") must contain(TTimestamp())
      db.types.testForTypes("2013-10-07 08:23:19.120") must contain(TTimestamp())


      db.loadTable("DETECTSERIESTEST1", new File("test/data/DetectSeriesTest1.csv"))

      val sch = 
        db.typechecker.schemaOf(
          db.table("DETECTSERIESTEST1")
        ).toMap

      sch must contain ("TRAN_TS" -> TTimestamp())

    }

  }


}
