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
 
      db.loadTable("CPUSPEED", new File("test/data/CPUSpeed.csv"))

      val baseTypes = db.bestGuessSchema(db.table("CPUSPEED_RAW")).toMap
      baseTypes.keys must contain(eachOf("CORES", "FAMILY", "TECH_MICRON"))
      baseTypes must contain("CORES" -> TString())
      baseTypes must contain("FAMILY" -> TString())
      baseTypes must contain("TECH_MICRON" -> TString())


      val lensTypes = db.bestGuessSchema(db.table("CPUSPEED")).toMap
      lensTypes.keys must contain(eachOf("CORES", "FAMILY", "TECH_MICRON"))
      lensTypes must contain("CORES" -> TInt())
      lensTypes must contain("FAMILY" -> TString())
      lensTypes must contain("TECH_MICRON" -> TFloat())

    }

    "Detect Timestamps Correctly" >> {

      Type.matches(TTimestamp(), "2014-06-15 08:23:19") must beTrue
      Type.matches(TTimestamp(), "2013-10-07 08:23:19.120") must beTrue


      db.loadTable("DETECTSERIESTEST1", new File("test/data/DetectSeriesTest1.csv"))

      val sch = 
        db.bestGuessSchema(
          db.table("DETECTSERIESTEST1")
        ).toMap

      sch must contain ("TRAN_TS" -> TTimestamp())

    }

  }


}
