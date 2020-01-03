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
      sourceFile = "test/data/geo.csv", 
      inferTypes = false, 
      detectHeaders =true
    )
  }
  
  "The Geocoding Lens" should {
    sequential 
    "Be able to do distance" >> {
 
      update("""
        CREATE LENS GEO_LENS_GOOGLE 
          AS SELECT * FROM ADDR
        WITH GEOCODE(HOUSE_NUMBER(STRNUMBER),STREET(STRNAME),CITY(CITY),STATE(STATE),GEOCODER(GOOGLE))
      """);

      val result = query("""
        SELECT ST_Point(LATITUDE, LONGITUDE ) AS pointshape FROM GEO_LENS_GOOGLE
      """)(results => results.toList.map( row =>  { 
        (
          row
        )
       }))
       
       println(result.mkString("\n"))
       
       ok
    }
  }
}