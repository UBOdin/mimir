package mimir.demo

import org.specs2.specification.core.{Fragment, Fragments}
import mimir.util._
import java.io.{BufferedReader, File, FileReader, StringReader}

import mimir.test._
import org.specs2.matcher.FileMatchers

object CureScenario
  extends SQLTestSpecification("CureScenario")
    with FileMatchers
{


  def time[A](description: String, op: () => A): A = {
    val t:StringBuilder = new StringBuilder()
    TimeUtils.monitor(description, op, println(_)) 
  }

  sequential


  val dataFiles = List(
    new File("test/data/cureSource.csv"),
    new File("test/data/cureLocations.csv")
  )

  "The CURE Scenario" should {

    "Import CSV's" >> {
      dataFiles.map( (x) => db.loadTable(x) )
      true
    }

    "CURE Timing Tests" >> {
      time("Type Inference Query",
             () => {
               query(
                 """
            SELECT * FROM cureSource;
                 """).foreachRow((x) => {})
             }
      )

       time("Update Source MV Lens",
         () => {
           update("""
             CREATE LENS MV1
             AS SELECT * FROM cureSource
             WITH MISSING_VALUE('IMO_CODE');
           """)
         }
       )

       time("Update Locations MV Lens",
         () => {
           update("""
             CREATE LENS MV2
             AS SELECT * FROM cureLocations
             WITH MISSING_VALUE('IMO_CODE');
           """)
         }
       )
/*
       time("CURE Query",
         () => {
           query("""
             SELECT *
             FROM   MV1 AS source
               JOIN MV2 AS locations
                       ON source.IMO_CODE = locations.IMO_CODE;
           """).foreachRow((x) => {})
         }
       )
*/
      time("Materalize MV1", () => {db.selectInto("MAT_MV1","MV1")})
      time("Materalize MV2", () => {db.selectInto("MAT_MV2","MV2")})

      time("CURE Query Materalized",
        () => {
          query("""
             SELECT *
             FROM   MAT_MV1 AS source
               JOIN MAT_MV2 AS locations
                       ON source.IMO_CODE = locations.IMO_CODE;
                """).foreachRow((x) => {})
        }
      )

      true
     }
  }
}