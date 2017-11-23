package mimir.models

import java.io._
import mimir.algebra._
import mimir.util._
import mimir.test._

object SparkClassifierModelSpec extends SQLTestSpecification("SparkClassifierTest")
{
  sequential

  var models = Map[String,(Model,Int,Seq[Expression])]()

  def predict(col:String, row:String): PrimitiveValue = {
    val (model, idx, hints) = models(col)
    model.bestGuess(idx, List(RowIdPrimitive(row)), List())
  }
  def explain(col:String, row:String): String = {
    val (model, idx, hints) = models(col)
    model.reason(idx, List(RowIdPrimitive(row)), List())
  }
  def trueValue(col:String, row:String): PrimitiveValue = {
    val t = db.tableSchema("CPUSPEED").get.find(_._1.equals(col)).get._2
    JDBCUtils.extractAllRows(
      db.backend.execute(s"SELECT $col FROM CPUSPEED WHERE ROWID=$row"),
      List(t)
    ).next.head
  }

  "The SparkClassifier Model" should {

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
      loadCSV("test/data/CPUSpeed.csv", allowAppend = true, typeInference = true)
      models = models ++ SparkClassifierModel.train(db, "CPUSPEEDREPAIR", List(
        "BUSSPEEDINMHZ"
      ), db.table("CPUSPEED"))
      models.keys must contain("BUSSPEEDINMHZ")
    }

    "Not choke when training multiple columns" >> {
      models = models ++ SparkClassifierModel.train(db, "CPUSPEEDREPAIR", List(
        "CORES",
        "TECHINMICRONS"
      ), db.table("CPUSPEED"))
      models.keys must contain("CORES", "TECHINMICRONS")
    }

    "Make reasonable predictions" >> {
      queryOneColumn("SELECT ROWID() FROM CPUSPEED"){ result =>
        val rowids = result.toSeq
        val predictions = 
          rowids.map {
            rowid => (
              predict("CORES", rowid.asString),
              trueValue("CORES", rowid.asString)
            )
          }
        val successes = 
          predictions.
            map( x => if(x._1.equals(x._2)){ 1 } else { 0 } ).
            fold(0)( _+_ )
        successes must be >=(rowids.size / 3)
      }
    }

    "Produce reasonable explanations" >> {

      explain("BUSSPEEDINMHZ", "3") must not contain("The classifier isn't willing to make a guess")
      explain("TECHINMICRONS", "22") must not contain("The classifier isn't willing to make a guess")
      explain("CORES", "20") must not contain("The classifier isn't willing to make a guess")

    }
  }

  "When combined with a TI Lens, the SparkClassifier Model" should {
    "Be trainable" >> {
      loadCSV("test/data/ratings1.csv", typeInference = true)
      val (model, idx, hints) = SparkClassifierModel.train(db,
        "RATINGS1REPAIRED", 
        List("RATING"), 
        db.table("RATINGS1")
      )("RATING")
      val nullRow = querySingleton("SELECT ROWID() FROM RATINGS1 WHERE RATING IS NULL")
      val guess = model.bestGuess(idx, List(nullRow), List()) 
      guess must beAnInstanceOf[FloatPrimitive] 
      guess must not be equalTo(FloatPrimitive(0.0))


    }
 
  }
}