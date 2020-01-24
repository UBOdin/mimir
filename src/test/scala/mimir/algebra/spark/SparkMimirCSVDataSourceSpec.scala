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
import mimir.algebra.BoolPrimitive
import mimir.test.TestTimer
import mimir.util.BackupUtils
import mimir.util.LoggerUtils
import mimir.data.FileFormat

object SparkMimirCSVDataSourceSpec 
  extends SQLTestSpecification("SparkMimirCSVDataSourceSpec")
  with TestTimer
{

 
  
 
  "MimirCSVDataSource" should {
    "Be able to load and query from a CSV source" >> {
      db.loader.loadTable(
        sourceFile = "test/r_test/r.csv", 
        targetTable = Some(ID("R")), 
        inferTypes = Some(true),
        detectHeaders = Some(true),
        format = FileFormat.CSV,
        sparkOptions = Map(
          "DELIMITER" -> ","
        ),
        datasourceErrors = true
      )
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
      db.loader.loadTable(
        sourceFile = "test/data/corrupt.csv", 
        targetTable = Some(ID("corrupt")), 
        inferTypes = Some(true),
        detectHeaders = Some(true),
        format = ID("csv"),
        sparkOptions = Map(
          "DELIMITER"        -> ",", 
          "datasourceErrors" -> "true"
        )
      )
      val resultDet = query("""
          SELECT * FROM CORRUPT
        """)(_.toList.map(row => {
          (row.tuple, row.tuple.zipWithIndex.map(el => row.isColDeterministic(el._2)).toList)
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
              List(false, true, true), 
              List(true, true, true), 
              List(true, false, false), 
              List(true, true, true)
           )
              
        }
        
        "Explain the errors" >> {
          val resultSets = db.uncertainty.explainEverything(table("CORRUPT"))
          val results = resultSets.flatMap(_.all(db)).map { _.toString }
          results must contain( eachOf(
            """There is an error(s) in the data source on row -735214268 of corrupt. The raw value of the row in the data source is [ ------- ] {{ MIMIR_DSE_WARNING_corrupt_DSE['-735214268'] }}""",
            """Couldn't Cast 'klj8' to int on row 1268020403 of corrupt._c1 {{ MIMIR_TI_WARNING_corrupt_TI['_c1', 'klj8', 'int', '1268020403'] }}""",
            """There is an error(s) in the data source on row 1268020403 of corrupt. The raw value of the row in the data source is [ 2,klj8,lmlkjh8,jij9,1 ] {{ MIMIR_DSE_WARNING_corrupt_DSE['1268020403'] }}""",
            """Couldn't Cast '-------' to int on row -735214268 of corrupt._c0 {{ MIMIR_TI_WARNING_corrupt_TI['_c0', '-------', 'int', '-735214268'] }}""",
            """Couldn't Cast 'lmlkjh8' to int on row 1268020403 of corrupt._c2 {{ MIMIR_TI_WARNING_corrupt_TI['_c2', 'lmlkjh8', 'int', '1268020403'] }}"""
           ))
        }
    }
    
    
    
    
    
    
  }
}