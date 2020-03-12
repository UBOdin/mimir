package mimir.algebra.spark

import java.io.File
import java.nio.file.Paths
import org.specs2.specification.BeforeAll
import mimir.exec.spark.datasource.google.spreadsheet.SparkSpreadsheetService

import mimir.algebra._
import mimir.algebra.NullPrimitive
import mimir.algebra.RowIdVar
import mimir.algebra.RowIdPrimitive
import mimir.algebra.Var
import mimir.algebra.StringPrimitive
import mimir.algebra.TInt
import mimir.algebra.Function
import mimir.algebra.AggFunction
import mimir.algebra.BoolPrimitive
import mimir.data.FileFormat
import mimir.exec.spark.MimirSpark
import mimir.exec.spark.MimirSparkRuntimeUtils
import mimir.test.SQLTestSpecification
import mimir.test.TestTimer
import mimir.util.BackupUtils

object GeoSparkSpec 
  extends SQLTestSpecification("GeoSparkSpec")
  with BeforeAll
{

  def beforeAll = 
  {
    loadCSV(
      targetTable = "ADDR",
      sourceFile = "test/data/geo_lat_lng.csv", 
      inferTypes = true, 
      detectHeaders =true
    )
  }
  
  "The Geospark functions" should {
    sequential 
    "Be able to do distance" >> {
    skipped("fixes required in MimirSpark.registerSparkFunctions"); ko
        
      
      /*val result = query("""
        SELECT ST_Centroid(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0)) centroid FROM ADDR
      """)(results => results.toList.map( row =>  { 
        (
          row
        )
       }))
     println(result.mkString("\n"))*/
       
       
      val result2 = query("""
        SELECT ST_Point(ADDR.LATITUDE, ADDR.LONGITUDE) AS pointshape FROM ADDR
      """)(results => results.toList.map( row =>  { 
        (
          row
        )
       }))
       
       //println(result2.mkString("\n"))
       
       ok
    }
  }
}