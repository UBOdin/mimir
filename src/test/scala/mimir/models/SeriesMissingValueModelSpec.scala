package mimir.models

import java.io._
import mimir.algebra._
import mimir.util._
import mimir.test._

object SeriesMissingValueModelSpec extends SQLTestSpecification("SeriesTest")
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
	  val rowid = row.toLong - 1
    val queryOper = select(s"SELECT $col FROM ORG_DETECTSERIESTEST3 WHERE ROWID=$rowid")
  	db.query(queryOper){ result =>
  		result.next.tuple(0)
  	}
  }

    "The SeriesMissingValue Model" should {

		"Be trainable" >> {
			loadCSV("test/data/DetectSeriesTest3.csv", typeInference = true)
			loadCSV("test/data/ORG_DetectSeriesTest3.csv", typeInference = true)
			models = models ++ SeriesMissingValueModel.train(db, "SERIESREPAIR", List(
			"AGE"
		  ), db.table("DETECTSERIESTEST3"))
		  models.keys must contain("AGE")
		}

		"Not choke when training multiple columns" >> {
			models = models ++ SeriesMissingValueModel.train(db, "SERIESREPAIR", List(
			"MARKETVAL", "GAMESPLAYED"
			), db.table("DETECTSERIESTEST3"))
			models.keys must contain("MARKETVAL", "GAMESPLAYED")
		}

		"Make reasonable predictions" >> {
		  queryOneColumn("SELECT ROWID() FROM DETECTSERIESTEST3 WHERE AGE IS NULL"){ result =>
  			val rowids = result.toIndexedSeq
  			val (predicted, correct) = 
  			  rowids.map { rowid => 
    				val a = predict("AGE", rowid.asString)
    				val b = trueValue("AGE", rowid.asString)
    				println(s"${rowid.asString}->$a,$b")
    				( (rowid -> a), (rowid -> b) )
  			  }.unzip
			
  			predicted.toMap must be equalTo(correct.toMap)
      }
		}
	
    "Produce reasonable explanations" >> {

      // explain("AGE", "1") must contain("I'm not able to guess based on weighted mean SERIESREPAIR:AGE.AGE, so defaulting using the upper and lower bound values")
      explain("AGE", "4") must contain("I interpolated SERIESREPAIR.AGE, ordered by SERIESREPAIR.DOB to get 23 for row '4'")
    }
  }
}	
	
	
	
