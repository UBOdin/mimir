package mimir.backend

import mimir.algebra.ID
import mimir.algebra.NullPrimitive
import mimir.algebra.Project
import mimir.algebra.ProjectArg
import mimir.algebra.RowIdVar
import mimir.algebra.Var
import mimir.test.SQLTestSpecification
import mimir.test.TestTimer
import mimir.util.LoggerUtils
import org.apache.spark.sql.types.LongType
import mimir.exec.spark.MimirSparkRuntimeUtils

object SparkBackendSpec 
  extends SQLTestSpecification("SparkBackendSpec")
  with TestTimer
{
 
  "SparkBackend" should {
    
    "Be able to zipWithIndex and RowIndexPlan with DetectHeaders " >> {
      db.loader.loadTable(
        sourceFile = "test/data/causes.csv", 
        targetTable = Some(ID("C")), 
        inferTypes = Some(true),
        detectHeaders = Some(true), //detect headers is breaking row ordering
        format = ID("csv"),
        sparkOptions = Map(
          "DELIMITER" -> ","
        ), 
        datasourceErrors = false
      )
      
      val tableOp = db.table(ID("C"))
      val tabledf = db.compiler.compileToSparkWithRewrites(tableOp)
      val idxdf = MimirSparkRuntimeUtils.zipWithIndex(tabledf, 2, "index", LongType)
      val cols = idxdf.schema.fields.map(_.name).toSeq
      val idxs = idxdf.collect().map(row => (row.getLong(row.fieldIndex("index")), Option(row.get(1)).getOrElse("null").toString())).toSeq
      
      cols must contain("index")
      idxs.head must be equalTo (2, "All Other Causes")
      idxs.last must be equalTo (1381, "All Other Causes")
      LoggerUtils.trace(
				//"mimir.backend.SparkBackend"
			){
        val rids = query("Select C.* FROM C;")(_.toList).map(row => (row.provenance.asLong, row(1).toString))
        rids.head must be equalTo (-1929017717, "'All Other Causes'")
        rids.last must be equalTo (-196590230, "'All Other Causes'")
      }
    }
    
    "Be able to zipWithIndex and RowIndexPlan on multi-partition datasets" >> {
      db.loader.loadTable(
        sourceFile = "test/data/JSONOUTPUTWIDE.csv", 
        targetTable = Some(ID("D")), 
        inferTypes = Some(true),
        detectHeaders = Some(false), //detect headers is breaking row ordering
        format = ID("csv"),
        sparkOptions = Map(
          "DELIMITER" -> ","
        ),
        datasourceErrors = true
      )
      
      val tableOp = db.table(ID("D"))
      val tabledf = db.compiler.compileToSparkWithRewrites(tableOp)
      val idxdf = MimirSparkRuntimeUtils.zipWithIndex(tabledf, 1, "index", LongType)
      val cols = idxdf.schema.fields.map(_.name).toSeq
      val idxs = idxdf.collect().map(row => (row.getLong(row.fieldIndex("index")), Option(row.get(1)).getOrElse("null").toString())).toSeq.drop(1)
      
      cols must contain("index")
      idxs.head must be equalTo (2, "748365939771641856")
      idxs.last must be equalTo (70715, "null")
      
      val rids = query("Select * FROM D;")(_.toList).map(row => (row.provenance.asLong, row(1).toString)).drop(1)
      rids.head must be equalTo (774558468, "748365939771641856")
      rids.last must be equalTo (551929072, "NULL")
    }
    
    "Be able to zipWithIndex and RowIndexPlan on single-partition datasets" >> {
      db.loader.loadTable(
        sourceFile = "test/data/pick.csv", 
        targetTable = Some(ID("P")), 
        inferTypes = Some(true),
        detectHeaders = Some(true),
        format = ID("csv"),
        sparkOptions = Map(
          "DELIMITER" -> ","
        ),
        datasourceErrors = false
      )
      
      val tableOp = db.table(ID("P"))
      val tabledf = db.compiler.compileToSparkWithoutRewrites(tableOp)
      val idxdf = MimirSparkRuntimeUtils.zipWithIndex(tabledf, 2, "index", LongType)
      val cols = idxdf.schema.fields.map(_.name).toSeq
      val idxs = idxdf.collect().map(row => (row.getLong(row.fieldIndex("index")), Option(row.get(0)).getOrElse("null").toString())).toSeq
      
      cols must contain("index")
      idxs.head must be equalTo (2, "1")
      idxs.last must be equalTo (5,"5")
      
      val rids = query("select * from P;")(_.toList).map(row => (row.provenance.asLong, row(0).toString))
      rids.head must be equalTo (-706855295, "1")
      rids.last must be equalTo (-1708392797,"5")
    }
    
    "Be able to use spark sql functions" >> {
      db.loader.loadTable(
        sourceFile = "test/data/geo.csv", 
        targetTable = Some(ID("geo")), 
        inferTypes = Some(true),
        detectHeaders = Some(true),
        format = ID("csv"),
        sparkOptions = Map(
          "DELIMITER" -> ","
        ),
        datasourceErrors = false
      )
      val funcres = query("Select substring(CITY, 0, 1) AS D FROM geo;")( results => {
        results.toList.map( el => el.tuple.toList)
      })
      //and not use aggregates that are registered already by mimir
      val aggres = query("Select COUNT(CITY) AS CC FROM geo;")( results => {
        results.toList.map( el => el.tuple.toList)
      })
      funcres must be equalTo List(List(str("B")),List(str("B")),List(str("B")),List(str("B")),List(str("B")),List(str("B")),List(str("S")))
      aggres must be equalTo List(List(i(7)))
    }
  }
}