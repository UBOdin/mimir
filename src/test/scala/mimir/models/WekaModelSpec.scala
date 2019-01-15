/*package mimir.models

import java.io._
import mimir.algebra._
import mimir.util._
import mimir.test._

object WekaModelSpec extends SQLTestSpecification("WekaTest")
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
    db.query(s"SELECT $col FROM CPUSPEED WHERE ROWID()=CAST($row AS rowid)")(
       result => result.toList.head(0)  
    )
  }

  "The Weka Model" should {

    "Be trainable" >> {
      loadCSV("CPUSPEED",  
      Seq(("PROCESSORID", "string"),
          ("FAMILY", "string"),
          ("TECHINMICRONS", "float"),
          ("CPUSPEEDINGHZ", "float"),
          ("BUSSPEEDINMHZ", "string"),
          ("L2CACHEINKB", "int"),
          ("L3CACHEINMB", "float"),
          ("CORES", "int"),
          ("EM64T", "string"),
          ("HT", "string"),
          ("VT", "string"),
          ("XD", "string"),
          ("SS", "string"),
          ("NOTES", "string")), new File("test/data/CPUSpeed.csv"))
      models = models ++ WekaModel.train(db, "CPUSPEEDREPAIR", List(
        "BUSSPEEDINMHZ"
      ), db.table("CPUSPEED"))
      models.keys must contain("BUSSPEEDINMHZ")
    }

    "Not choke when training multiple columns" >> {
      models = models ++ WekaModel.train(db, "CPUSPEEDREPAIR", List(
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

  "When combined with a TI Lens, the Weka Model" should {
    "Be trainable" >> {
      db.loadTable("RATINGS1", new File("test/data/ratings1.csv"))
      val (model, idx, hints) = WekaModel.train(db,
        "RATINGS1REPAIRED", 
        List("RATING"), 
        db.table("RATINGS1")
      )("RATING")
      val nullRow = querySingleton("SELECT ROWID() FROM RATINGS1 WHERE RATING IS NULL")
      model.bestGuess(idx, List(nullRow), List()) must beAnInstanceOf[FloatPrimitive]


    }
 
  }
}*/