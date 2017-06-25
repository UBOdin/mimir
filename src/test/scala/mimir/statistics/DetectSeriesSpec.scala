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
	extends SQLTestSpecification("tempDBDemoScript"){
	
	val testDS = new DetectSeries(db)
	def testDetectSeriesof(oper: Operator) = testDS.detectSeriesOf(oper)
	def testGetSeriesof(tabName: String) = {
		testDS.detectSeriesOf(db)
		testDS.getSeriesof(tabName)
	}
	
	"The DetectSeriesSpec" should {

		"Be able to load DetectSeriesTest1" >> {
			db.loadTable("test/data/DetectSeriesTest1.csv"); ok
		}
		
		"Be able to detect Date and Timestamp type" >> {
			val queryOper = select("SELECT * FROM DetectSeriesTest1")
			val colSeq: Seq[String] = testDetectSeriesof(queryOper).map{_.columnName.toString}
			
			colSeq must have size(4)
			colSeq must contain("Exp_DT", "Join_DT", "DOB", "Tran_TS")
		}
		
		"Be able to get Series from table name" >> {
			val colSeq1: Seq[String] =	testGetSeriesof("DetectSeriesTest1").map{_.columnName.toString}		

			colSeq1 must have size(4)
			colSeq1 must contain("Exp_DT", "Join_DT", "DOB", "Tran_TS")
		}
	}	
}