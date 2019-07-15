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
 
      db.loader.loadTable(
        targetTable = Some(ID("CPUSPEED")), 
        sourceFile = "test/data/CPUSpeed.csv"
      )

      val baseTypes = db.typechecker.schemaOf(db.table("CPUSPEED_RAW")).toMap
      baseTypes must contain(ID("_c7") -> TString())
      baseTypes must contain(ID("_c1") -> TString())
      baseTypes must contain(ID("_c2") -> TString())


      val lensTypes = db.typechecker.schemaOf(db.table("CPUSPEED")).toMap
      lensTypes must contain(ID("CORES") -> TInt())
      lensTypes must contain(ID("FAMILY") -> TString())
      lensTypes must contain(ID("TECH_MICRON") -> TFloat())

    }

    "Detect Timestamps Correctly" >> {

      Type.matches(TTimestamp(), "2014-06-15 08:23:19") must beTrue
      Type.matches(TTimestamp(), "2013-10-07 08:23:19.120") must beTrue


      db.loader.loadTable(
        targetTable = Some(ID("DETECTSERIESTEST1")), 
        sourceFile = "test/data/DetectSeriesTest1.csv"
      )

      val sch = 
        db.typechecker.schemaOf(
          db.table("DETECTSERIESTEST1")
        ).toMap

      sch must contain (ID("TRAN_TS") -> TTimestamp())

    }

  }


}
