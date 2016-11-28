package mimir.models

import java.io._
import mimir.algebra._
import mimir.util._

object WekaModelSpec extends SQLTestSpecification("WekaTest")
{
  sequential

  var models = Map[String,(Model,Int)]()

  "Intializing" should {

    "Load CPU Speed Data" >> {
      update("""
        CREATE TABLE cpuspeed(
          PROCESSORID string,
          FAMILY string,
          TECHINMICRONS decimal,
          CPUSPEEDINGHZ decimal,
          BUSSPEEDINMHZ decimal,
          L2CACHEINKB int,
          L3CACHEINMB int,
          CORES int,
          EM64T string,
          HT string,
          VT string,
          XD string,
          SS string,
          NOTES string,
          PRIMARY KEY(processorID)
        )
      """)
      loadCSV("cpuspeed", new File("test/data/CPUSpeed.csv"))
      true
    }

  }

  "The Weka Model" should {

    "Be Trainable" >> {
      models = models ++ WekaModel.train(db, "CPUSPEEDREPAIR", List(
        "BUSSPEEDINMHZ"
      ), db.getTableOperator("CPUSPEED"))
      models.keys must contain("BUSSPEEDINMHZ")
    }

    "Support Multiple Columns" >> {
      models = models ++ WekaModel.train(db, "CPUSPEEDREPAIR", List(
        "CORES",
        "EM64T"
      ), db.getTableOperator("CPUSPEED"))
      models.keys must contain("CORES", "EM64T")
    }

  }
}