package mimir.algebra.spark

import mimir.algebra._
import org.specs2.specification.BeforeAll
import mimir.test.SQLTestSpecification
import mimir.algebra.NullPrimitive
import mimir.algebra.RowIdVar
import mimir.algebra.RowIdPrimitive
import mimir.algebra.Var
import mimir.algebra.StringPrimitive
import mimir.algebra.TInt
import java.io.File
import mimir.algebra.Function
import mimir.algebra.AggFunction
import mimir.util.LoadJDBC
import mimir.algebra.BoolPrimitive
import mimir.test.TestTimer
import mimir.sql.RABackend
import mimir.util.BackupUtils
import mimir.util.LoggerUtils

object SparkMimirCSVDataSourceSpec 
  extends SQLTestSpecification("SparkMimirCSVDataSourceSpec")
  with TestTimer
{

 
  
 
  "MimirCSVDataSource" should {
    "Be able to load and query from a CSV source" >> {
      db.loadTable(
        "r", 
        new File("test/r_test/r.csv"), 
        true, 
        None,
        true,
        true,
        Map("DELIMITER" -> ",", "datasourceErrors" -> "true"),
        "csv")
      val result = query("""
        SELECT * FROM R
      """)(_.toList.map(_.tuple))
      
      result must be equalTo List(
       List(i(1), i(2), i(3)), 
       List(i(1), i(3), i(1)), 
       List(i(2), NullPrimitive(), i(1)), 
       List(i(1), i(2), NullPrimitive()), 
       List(i(1), i(4), i(2)), 
       List(i(2), i(2), i(1)), 
       List(i(4), i(2), i(4))   
      )
    }
    
    "Be able to load and query from a CSV source that contains errors and" >> {
      db.loadTable(
        "corrupt", 
        new File("test/data/corrupt.csv"), 
        true, 
        None,
        true,
        true,
        Map("DELIMITER" -> ",", "datasourceErrors" -> "true"),
        "csv")
      val resultDet = query("""
          SELECT * FROM CORRUPT
        """)(_.toList.map(row => {
          (row.tuple, row.tuple.zipWithIndex.map(el => row.isColDeterministic(el._2)))
        }).unzip)
        
        val result = resultDet._1
        
        
        result must be equalTo List(
         List(i(1), i(2), i(3)), 
         List(i(1), i(3), i(1)), 
         List(i(2), NullPrimitive(), i(1)), 
         List(NullPrimitive(), NullPrimitive(), NullPrimitive()), 
         List(i(1), i(4), i(2)), 
         List(i(2), NullPrimitive(), NullPrimitive()), 
         List(i(4), i(2), i(4))   
        )
        
        "Appropriatly assign determinism for rows with errors" >> {
          val det = resultDet._2
          det must be equalTo List(
           List(true, true, true), 
           List(true, true, true), 
           List(true, true, true), 
           List(false, false, false), 
           List(true, true, true), 
           List(false, false, false), 
           List(true, true, true)   
          )
        }
        
        "Explain the errors" >> {
          val resultSets = db.explainer.explainEverything(table("CORRUPT"))
          resultSets.map(_.all(db).map(_.toJSON)).flatten must contain(eachOf(
              """{"rowidarg":-1,"source":"MIMIR_DSE_WARNING_CORRUPT_DSE","confirmed":false,"varid":0,"english":"The value [ NULL ] is uncertain because there is an error(s) in the data source on row 4. The raw value of the row in the data source is [ ------- ]","repair":{"selector":"warning"},"args":["'_c1'","NULL","'-------'"]}""", 
              """{"rowidarg":-1,"source":"MIMIR_DSE_WARNING_CORRUPT_DSE","confirmed":false,"varid":0,"english":"The value [ NULL ] is uncertain because there is an error(s) in the data source on row 4. The raw value of the row in the data source is [ ------- ]","repair":{"selector":"warning"},"args":["'_c2'","NULL","'-------'"]}""", 
              """{"rowidarg":-1,"source":"MIMIR_DSE_WARNING_CORRUPT_DSE","confirmed":false,"varid":0,"english":"The value [ klj8 ] is uncertain because there is an error(s) in the data source on row 6. The raw value of the row in the data source is [ 2,klj8,lmlkjh8,jij9,1 ]","repair":{"selector":"warning"},"args":["'_c1'","'klj8'","'2,klj8,lmlkjh8,jij9,1'"]}""", 
              """{"rowidarg":-1,"source":"MIMIR_DSE_WARNING_CORRUPT_DSE","confirmed":false,"varid":0,"english":"The value [ ------- ] is uncertain because there is an error(s) in the data source on row 4. The raw value of the row in the data source is [ ------- ]","repair":{"selector":"warning"},"args":["'_c0'","'-------'","'-------'"]}""", 
              """{"rowidarg":-1,"source":"MIMIR_DSE_WARNING_CORRUPT_DSE","confirmed":false,"varid":0,"english":"The value [ lmlkjh8 ] is uncertain because there is an error(s) in the data source on row 6. The raw value of the row in the data source is [ 2,klj8,lmlkjh8,jij9,1 ]","repair":{"selector":"warning"},"args":["'_c2'","'lmlkjh8'","'2,klj8,lmlkjh8,jij9,1'"]}""", 
              """{"rowidarg":-1,"source":"MIMIR_DSE_WARNING_CORRUPT_DSE","confirmed":false,"varid":0,"english":"The value [ 2 ] is uncertain because there is an error(s) in the data source on row 6. The raw value of the row in the data source is [ 2,klj8,lmlkjh8,jij9,1 ]","repair":{"selector":"warning"},"args":["'_c0'","'2'","'2,klj8,lmlkjh8,jij9,1'"]}"""))
        }
    }
    
    
    
    
    
    
  }
}