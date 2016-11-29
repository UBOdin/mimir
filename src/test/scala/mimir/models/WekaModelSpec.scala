package mimir.models

import java.io._
import mimir.algebra._
import mimir.util._

object WekaModelSpec extends SQLTestSpecification("WekaTest")
{
  sequential

  var models = Map[String,(Model,Int)]()

  def predict(col:String, row:String): String = {
    val (model, idx) = models(col)
    model.bestGuess(idx, List(RowIdPrimitive(row))).asString
  }
  def trueValue(col:String, row:String): String =
    db.backend.resultValue(s"SELECT $col FROM CPUSPEED WHERE ROWID=$row").asString


  "Intializing" should {

    "Load CPU Speed Data" >> {
      update("""
        CREATE TABLE CPUSPEED(
          PROCESSORID string,
          FAMILY string,
          TECHINMICRONS decimal,
          CPUSPEEDINGHZ decimal,
          BUSSPEEDINMHZ string,
          L2CACHEINKB int,
          L3CACHEINMB decimal,
          CORES int,
          EM64T string,
          HT string,
          VT string,
          XD string,
          SS string,
          NOTES string
        )
      """)
      loadCSV("CPUSPEED", new File("test/data/CPUSpeed.csv"))
      true
    }

  }

  "The Weka Model" should {

    "Be trainable" >> {
      models = models ++ WekaModel.train(db, "CPUSPEEDREPAIR", List(
        "BUSSPEEDINMHZ"
      ), db.getTableOperator("CPUSPEED"))
      models.keys must contain("BUSSPEEDINMHZ")
    }

    "Not choke when training multiple columns" >> {
      models = models ++ WekaModel.train(db, "CPUSPEEDREPAIR", List(
        "CORES",
        "EM64T"
      ), db.getTableOperator("CPUSPEED"))
      models.keys must contain("CORES", "EM64T")
    }

    "Make reasonable predictions" >> {

      predict("BUSSPEEDINMHZ", "3") must be equalTo trueValue("BUSSPEEDINMHZ", "3")

    }

  }
}