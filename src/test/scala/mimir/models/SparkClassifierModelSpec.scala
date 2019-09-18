package mimir.models

import java.io._
import mimir.algebra._
import mimir.util._
import mimir.test._

object SparkClassifierModelSpec extends SQLTestSpecification("SparkClassifierTest")
{
  sequential

  var models = Map[ID,(Model,Int,Seq[Expression])]()

  def predict(col:ID, row:String): PrimitiveValue = {
    val (model, idx, hints) = models(col)
    model.bestGuess(idx, List(RowIdPrimitive(row)), List())
  }
  def explain(col:ID, row:String): String = {
    val (model, idx, hints) = models(col)
    model.reason(idx, List(RowIdPrimitive(row)), List())
  }
  def trueValue(col:ID, row:String): PrimitiveValue = {
    val t = db.tableSchema("CPUSPEED").get.find(_._1.equals(col)).get._2
    db.query(
      db.table("CPUSPEED")
        .addColumns( "ROWID" -> RowIdVar())
        .projectByID(col, ID("ROWID"))
        .filter(Var(ID("ROWID")).eq(mimir.algebra.Cast(TRowId(), StringPrimitive(row)))))(
      result => result.toList.head(0)
    )
  }

  "The SparkClassifier Model" should {
    /*db.loadTable("CPUSPEED", new File("test/data/CPUSpeed.csv"))
    val inputOp = db.table("CPUSPEED").rename(
          ("PROCESSOR_NUMBER","PROCESSORID"), 
          ("TECH_MICRON","TECHINMICRONS"), 
          ("CPU_SPEED_GHZ","CPUSPEEDINGHZ"), 
          ("BUS_SPEED_MHZ","BUSSPEEDINMHZ"), 
          ("L2_CACHE_SIZE_KB","L2CACHEINKB"), 
          ("L3_CACHE_SIZE_MB","L3CACHEINMB"))*/
    
    "Be trainable" >> {
      loadCSV(
        targetTable = "CPUSPEED",  
        sourceFile = "test/data/CPUSpeed.csv",
        targetSchema = Seq(
          "PROCESSORID",
          "FAMILY",
          "TECHINMICRONS",
          "CPUSPEEDINGHZ",
          "BUSSPEEDINMHZ",
          "L2CACHEINKB",
          "L3CACHEINMB",
          "CORES",
          "EM64T",
          "HT",
          "VT",
          "XD",
          "SS",
          "NOTES"
        )
      )
     
      models = models ++ SparkClassifierModel.train(db, ID("CPUSPEEDREPAIR"), List(
        ID("BUSSPEEDINMHZ")
      ), db.table("CPUSPEED"), "CPUSPEEDREPAIR_BUS")
      models.keys must contain(ID("BUSSPEEDINMHZ"))
    }

    "Not choke when training multiple columns" >> {
      models = models ++ SparkClassifierModel.train(db, ID("CPUSPEEDREPAIR"), List(
        ID("CORES"),
        ID("TECHINMICRONS")
      ), db.table("CPUSPEED"),"CPUSPEEDREPAIR_CORES")
      models.keys must contain(eachOf(ID("CORES"), ID("TECHINMICRONS")))
    }

    "Make reasonable predictions" >> {
      db.query(
        db.table("CPUSPEED")
          .addColumns( "ROWID" -> RowIdVar())
          .projectByID(ID("CORES"), ID("ROWID"))
          .filter(Var(ID("CORES")).isNull))(
        result => {
              val rowids = result.toSeq
              val predictions = 
                rowids.map {
                  rowid => (
                    predict(ID("CORES"), rowid(0).asString),
                    trueValue(ID("CORES"), rowid(0).asString)
                  )
                }
              val successes = 
                predictions.
                  map( x => if(x._1.equals(x._2)){ 1 } else { 0 } ).
                  fold(0)( _+_ )
              successes must be >=(rowids.size / 3)
            }
        )
    }
  }

  "When combined with a TI Lens, the SparkClassifier Model" should {
    "Be trainable" >> {
      db.loader.loadTable(
        targetTable = Some(ID("RATINGS1")), 
        sourceFile = "test/data/ratings1.csv"
      )
      val (model, idx, hints) = SparkClassifierModel.train(db,
        ID("RATINGS1REPAIRED"), 
        List(ID("RATING")), 
        db.table("RATINGS1"),
        "RATINGS1REPAIRED_RATING"
      )(ID("RATING"))
      val nullRow = querySingleton("SELECT ROWID() FROM RATINGS1 WHERE RATING IS NULL")
      //println(nullRow)
      val guess = model.bestGuess(idx, List(nullRow), List()) 
      guess must beAnInstanceOf[FloatPrimitive] 
      guess must not be equalTo(FloatPrimitive(0.0))


    }
 
  }
}