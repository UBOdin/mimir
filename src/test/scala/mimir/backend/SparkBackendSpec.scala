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

object SparkBackendSpec 
  extends SQLTestSpecification("SparkBackendSpec")
  with TestTimer
{
 
  "SparkBackend" should {
     "Be able to zipWithIndex" >> {
      db.loadTable(
        sourceFile = "test/data/JSONOUTPUTWIDE.csv", 
        targetTable = Some(ID("D")), 
        force = true, 
        targetSchema = None,
        inferTypes = Some(true),
        detectHeaders = Some(true),
        format = ID("csv"),
        loadOptions = Map(
          "DELIMITER" -> ",", 
          "datasourceErrors" -> "true"
        )
      )
      
      val tableOp = db.table(ID("D"))
      /*val projOp = Project(Seq(ProjectArg(ID("RID"),RowIdVar())), tableOp)
      println(db.query(tableOp)(_.toList).take(300).map(_.provenance).mkString("\n"))
      println(db.query(projOp)(_.toList).take(300).map(row => row.tuple.mkString(",") +","+ row.provenance).mkString("\n"))
      println(query("Select ROWID() AS RID FROM D;")(_.toList).take(300).map(row => row.tuple.mkString(",") ).mkString("\n"))*/
      
      val tabledf = db.backend.execute(tableOp)
      val idxdf = db.backend.asInstanceOf[SparkBackend].zipWithIndex(tabledf, 1, "index", LongType)
      val cols = idxdf.schema.fields.map(_.name).toSeq
      val idxs = idxdf.collect().map(row => row.getLong(row.fieldIndex("index"))).toSeq
      
      cols must contain("index")
      idxs.head must be equalTo 1
      idxs.last must be equalTo 70714
      
    }
    "Be able to use spark sql functions" >> {
      db.loadTable(
        sourceFile = "test/data/geo.csv", 
        targetTable = Some(ID("geo")), 
        force = true, 
        targetSchema = None,
        inferTypes = Some(true),
        detectHeaders = Some(true),
        format = ID("csv"),
        loadOptions = Map(
          "DELIMITER" -> ",", 
          "datasourceErrors" -> "true"
        )
      )
      val funcres = query("Select subsrting(CITY, 0, 1) AS D FROM geo;")( results => {
        results.toList.map( el => el.tuple.toList)
      })
      funcres must be equalTo List(List(str("B")),List(str("S")),List(str("B")),List(str("B")),List(str("B")),List(str("B")),List(str("B")))
    }
  }
}