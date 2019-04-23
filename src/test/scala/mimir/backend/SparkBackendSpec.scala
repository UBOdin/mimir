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


object SparkBackendSpec 
  extends SQLTestSpecification("SparkMimirCSVDataSourceSpec")
  with TestTimer
{

 
  
 
  "SparkBackend" should {
    "Be able to zipWithIndex" >> {
      db.loadTable(
        sourceFile = "test/data/JSONOUTPUTWIDE.csv", 
        targetTable = Some(ID("D")), 
        force = true, 
        targetSchema = None,
        inferTypes = Some(false),
        detectHeaders = Some(true),
        format = ID("csv"),
        loadOptions = Map(
          "DELIMITER" -> ",", 
          "datasourceErrors" -> "true"
        )
      )
      
      val sparkBackend = db.backend.asInstanceOf[SparkBackend]
      val tableOp = db.table(ID("D_RAW"))
      
      val tabledf = db.backend.execute(tableOp)
      val idxdf = sparkBackend.zipWithIndex(tabledf)
      val cols = idxdf.schema.fields.map(_.name).toSeq
      val idxs = idxdf.collect().map(row => row.getLong(row.fieldIndex("index"))).toSeq
      
      cols must contain("index")
      idxs.head must be equalTo 1
      idxs.last must be equalTo 70715
      
    }
  }
}