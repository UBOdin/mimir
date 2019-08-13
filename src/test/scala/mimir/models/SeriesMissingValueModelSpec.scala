package mimir.models

import java.io._
import mimir.algebra._
import mimir.util._
import mimir.test._

object SeriesMissingValueModelSpec extends SQLTestSpecification("SeriesTest")
{
  sequential

  var models = Map[ID,(Model,Int,Seq[Expression])]()
  val rowidMap = 
    Map("-737200991"->"384450702",
        "1538020126"->"-1330496373",
        "196110172"->"1034334144",
        "1067514362"->"59129355")
        
  def predict(col:ID, row:String): PrimitiveValue = {
    val (model, idx, hints) = models(col)
    model.bestGuess(idx, List(RowIdPrimitive(rowidMap(row))), List())
  }
  def explain(col:ID, row:String): String = {
    val (model, idx, hints) = models(col)
    model.reason(idx, List(RowIdPrimitive(rowidMap(row))), List())
  }

  def trueValue(col:ID, row:String): PrimitiveValue = {
	  val queryOper = select(s"SELECT $col FROM ORG_DETECTSERIESTEST3 WHERE ROWID=${rowidMap(row)}")
  	db.query(queryOper){ result =>
  		result.next.tuple(0)
  	}
  }
  
  

  "The SeriesMissingValue Model" should {

		"Be trainable" >> {
			loadCSV("test/data/DetectSeriesTest3.csv")
			loadCSV("test/data/ORG_DetectSeriesTest3.csv")
			models = models ++ SeriesMissingValueModel.train(db, ID("SERIESREPAIR"), List(
			ID("AGE")
		  ), db.table("DETECTSERIESTEST3"), "MyTestCaseDataset")
		  models.keys must contain(ID("AGE"))
		}

		"Not choke when training multiple columns" >> {
			mimir.util.LoggerUtils.trace(
				 //"mimir.models.SeriesMissingValueModel"
			){
		  models = models ++ SeriesMissingValueModel.train(db, ID("SERIESREPAIR"), List(
			ID("MARKETVAL"), ID("GAMESPLAYED")
			), db.table("DETECTSERIESTEST3"), "MyTestCaseDataset")
			models.keys must contain(ID("MARKETVAL"), ID("GAMESPLAYED"))
			}
		}

		"Make reasonable predictions" >> {
		   mimir.util.LoggerUtils.trace(
				 //"mimir.sql.SparkBackend",
		     // "mimir.models.SeriesMissingValueModel"  
			) {
		  queryOneColumn("SELECT ROWID() FROM DETECTSERIESTEST3 WHERE AGE IS NULL"){ result =>
  			val rowids = result.toIndexedSeq
  			val (predicted, correct) = 
  			  rowids.map { rowid => 
    				val a = predict(ID("AGE"), rowid.asString)
    				val b = trueValue(ID("AGE"), rowid.asString)
    				//println(s"${rowid.asString}->$a,$b")
    				( (rowid -> a), (rowid -> b) )
  			  }.unzip
  			predicted.toMap must be equalTo(correct.toMap)
      }
		   }
		}
	
    "Produce reasonable explanations" >> {

      // explain("AGE", "1") must contain("I'm not able to guess based on weighted mean SERIESREPAIR:AGE.AGE, so defaulting using the upper and lower bound values")
      explain(ID("AGE"), "4") must contain("I interpolated MyTestCaseDataset.AGE, ordered by MyTestCaseDataset.DOB to get 23 for row '4'")
    }
  }
}	
	
	
	
