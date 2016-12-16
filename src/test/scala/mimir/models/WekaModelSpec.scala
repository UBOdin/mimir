package mimir.models

import java.io._
import mimir.algebra._
import mimir.util._
import mimir.test._

object WekaModelSpec extends SQLTestSpecification("WekaTest")
{
  sequential

  var models = Map[String,(Model,Int)]()

  def predict(col:String, row:String): PrimitiveValue = {
    val (model, idx) = models(col)
    model.bestGuess(idx, List(RowIdPrimitive(row)))
  }
  def trueValue(col:String, row:String): PrimitiveValue = {
    val t = db.getTableSchema("CPUSPEED").get.find(_._1.equals(col)).get._2
    JDBCUtils.extractAllRows(
      db.backend.execute(s"SELECT $col FROM CPUSPEED WHERE ROWID=$row"),
      List(t)
    ).next.head
  }

  "The Weka Model" should {

    "Be trainable" >> {
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
      models = models ++ WekaModel.train(db, "CPUSPEEDREPAIR", List(
        "BUSSPEEDINMHZ"
      ), db.getTableOperator("CPUSPEED"))
      models.keys must contain("BUSSPEEDINMHZ")
    }

    "Not choke when training multiple columns" >> {
      models = models ++ WekaModel.train(db, "CPUSPEEDREPAIR", List(
        "CORES",
        "TECHINMICRONS"
      ), db.getTableOperator("CPUSPEED"))
      models.keys must contain("CORES", "TECHINMICRONS")
    }

    "Make reasonable predictions" >> {

      predict("BUSSPEEDINMHZ", "3") must be equalTo trueValue("BUSSPEEDINMHZ", "3")
      predict("TECHINMICRONS", "22") must be equalTo trueValue("TECHINMICRONS", "22")
      predict("CORES", "20") must be equalTo trueValue("CORES", "20")

    }
  }

  "When combined with a TI Lens, the Weka Model" should {
    "Be trainable" >> {
      db.loadTable("RATINGS1", new File("test/data/ratings1.csv"))
      val (model, idx) = WekaModel.train(db,
        "RATINGS1REPAIRED", 
        List("RATING"), 
        db.getTableOperator("RATINGS1")
      )("RATING")
      val nullRow = querySingleton("SELECT ROWID FROM RATINGS1 WHERE RATING IS NULL")
      model.bestGuess(idx, List(nullRow)) must beAnInstanceOf[FloatPrimitive]


    }
 
  }
}