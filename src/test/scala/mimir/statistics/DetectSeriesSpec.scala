package mimir.statistics

import java.io.{BufferedReader, File, FileReader, StringReader}
import java.sql.SQLException

import scala.collection.JavaConversions._
import org.specs2.mutable._
import org.specs2.matcher.FileMatchers
import mimir._
import mimir.sql._
import mimir.parser._
import mimir.algebra._
import mimir.optimizer._
import mimir.ctables._
import mimir.exec._
import mimir.util._
import mimir.test._
import mimir.statistics._
import net.sf.jsqlparser.statement.Statement

object DetectSeriesSpec 
	extends SQLTestSpecification("DetectSeriesTest"){
	
	sequential
	
	val testDS = new DetectSeries(db, 0.1)
	def testDetectSeriesof(oper: Operator) = testDS.detectSeriesOf(oper)
	
	
	"The DetectSeriesSpec" should {

		"Be able to load DetectSeriesTest1" >> {
			db.loadTable("test/data/DetectSeriesTest1.csv"); ok
		}
		
		"Be able to detect Date and Timestamp type" >> {
			val queryOper = select("SELECT * FROM DetectSeriesTest1")
			val colSeq: Seq[String] = testDetectSeriesof(queryOper).map{_.columnName.toString}
			
			colSeq must have size(4)
			colSeq must contain("TRAN_TS","EXP_DT", "JOIN_DT", "DOB")
		}

		"Be able to create a new schema and detect Date and Timestamp type" >> {
			update("CREATE TABLE DetectSeriesTest3(JN_DT date, JN_TS datetime)")
			val queryOper = select("SELECT * FROM DetectSeriesTest3")
			val colSeq: Seq[String] = testDetectSeriesof(queryOper).map{_.columnName.toString}
			
			colSeq must have size(2)
			colSeq must contain("JN_DT", "JN_TS")
		}

		"Be able to load DetectSeriesTest2" >> {
			db.loadTable("test/data/DetectSeriesTest2.csv"); ok
		}

		"Be able to detect Date, Timestamp and increasing-decreasing Numeric type" >> {
			val queryOper = select("SELECT * FROM DetectSeriesTest2")
			val colSeq: Seq[String] = testDetectSeriesof(queryOper).map{_.columnName.toString}
			
			colSeq must have size(6)
			colSeq must contain("TRAN_TS","EXP_DT", "JOIN_DT", "DOB", "ROW_ID", "QUALITY")
		}
		
	}	
}