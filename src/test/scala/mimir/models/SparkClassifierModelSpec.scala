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
    db.query(db.table("CPUSPEED").addColumn(("ROWID", RowIdVar())).project(col, "ROWID").filter(Var("ROWID").eq(mimir.algebra.Cast(TRowId(), StringPrimitive(row)))))(
      result => result.toList.head(0)
    )
  }

  "The SparkClassifier Model" should {
    db.loadTable("CPUSPEED", new File("test/data/CPUSpeed.csv"))
    val inputOp = db.table("CPUSPEED").rename(
          ("PROCESSOR_NUMBER","PROCESSORID"), 
          ("TECH_MICRON","TECHINMICRONS"), 
          ("CPU_SPEED_GHZ","CPUSPEEDINGHZ"), 
          ("BUS_SPEED_MHZ","BUSSPEEDINMHZ"), 
          ("L2_CACHE_SIZE_KB","L2CACHEINKB"), 
          ("L3_CACHE_SIZE_MB","L3CACHEINMB"))
    
    "Be trainable" >> {
      /*LoadCSV.handleLoadTableRaw(db, "CPUSPEED",  
      Seq(("PROCESSORID", TString()),
          ("FAMILY", TString()),
          ("TECHINMICRONS", TFloat()),
          ("CPUSPEEDINGHZ", TFloat()),
          ("BUSSPEEDINMHZ", TString()),
          ("L2CACHEINKB", TInt()),
          ("L3CACHEINMB", TFloat()),
          ("CORES", TInt()),
          ("EM64T", TString()),
          ("HT", TString()),
          ("VT", TString()),
          ("XD", TString()),
          ("SS", TString()),
          ("NOTES", TString())), new File("test/data/CPUSpeed.csv"), Map("headers" -> "true"))
     */
      models = models ++ SparkClassifierModel.train(db, "CPUSPEEDREPAIR", List(
        "BUSSPEEDINMHZ"
      ), inputOp)
      models.keys must contain("BUSSPEEDINMHZ")
    }

    "Not choke when training multiple columns" >> {
      models = models ++ SparkClassifierModel.train(db, "CPUSPEEDREPAIR", List(
        "CORES",
        "TECHINMICRONS"
      ), inputOp)
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
        println(predictions.toList)
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
      db.loadTable("RATINGS1", new File("test/data/ratings1.csv"))
      val (model, idx, hints) = SparkClassifierModel.train(db,
        "RATINGS1REPAIRED", 
        List("RATING"), 
        db.table("RATINGS1")
      )("RATING")
      val nullRow = querySingleton("SELECT ROWID() FROM RATINGS1 WHERE RATING IS NULL")
      println(nullRow)
      val guess = model.bestGuess(idx, List(nullRow), List()) 
      guess must beAnInstanceOf[FloatPrimitive] 
      guess must not be equalTo(FloatPrimitive(0.0))


    }
 
  }
}