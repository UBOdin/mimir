package mimir.demo

import java.io._
import org.specs2.reporter.LineLogger
import org.specs2.specification.core.{Fragment,Fragments}

import mimir.test._
import mimir.util._
import LoggerUtils.trace
import mimir.ctables._
import mimir.algebra.{NullPrimitive,MissingVariable}

object CureScenario
  extends SQLTestSpecification("CureScenario",  Map("reset" -> "YES"))
{
  // This test case should be checked at least once per major version
  // but is reeeeeally slow and memory intensive.  Skip it in general
  args(skipAll = true)
  
  val dataFiles = List(
    "test/data/cureSource.csv",
    "test/data/cureLocations.csv",
    "test/data/curePorts.csv"
  )

  val cureQuery = """
    SELECT BILL_OF_LADING_NBR,
           SRC.IMO_CODE           AS "SRC_IMO",
           LOC.LAT                AS "VESSEL_LAT",
           LOC.LON                AS "VESSEL_LON",
           PORTS.LAT              AS "PORT_LAT",
           PORTS.LON              AS "PORT_LON",
           DATE(SRC.DATE)          AS "SRC_DATE",
           DST(LOC.LON, LOC.LAT, PORTS.LON, PORTS.LAT) AS "DISTANCE",  SPEED(DST(LOC.LON, LOC.LAT, PORTS.LON, PORTS.LAT), SRC.DATE, DATE('2016-09-03')) AS "SPEED"
    FROM MV1 AS SRC
      JOIN MV2 AS LOC ON SRC.IMO_CODE = LOC.IMO_CODE
        LEFT OUTER JOIN CUREPORTS AS PORTS ON SRC.PORT_OF_ARRIVAL = PORTS.PORT
        WHERE SPEED(DST(LOC.LON, LOC.LAT, PORTS.LON, PORTS.LAT),SRC.DATE, DATE('2016-09-03')) > 100;
    ;"""

  def time[A](description: String): ( => A) => A =
    Timer.monitor(description)

  sequential 

  "The CURE Scenario" should {
    Fragment.foreach(dataFiles){ table => {
      val basename = new File(table).getName().replace(".csv", "").toUpperCase
      s"Load '$table'" >> {
        time(s"Load '$table'") {
          //update(s"LOAD '$table';") 
          loadCSV(table)
        }
        // time(s"Materialize '$basename'"){
        //   update(s"ALTER VIEW $basename MATERIALIZE;")
        // }
        db.uncertainty.explainEverything(
          db.table(basename)) must not beEmpty;
        db.tableExists(basename) must beTrue
      }
    }}

    "Select from the source table" >> {
      time("Source Query"){ 
      query("""
          SELECT * FROM cureSource;
        """){ _.foreach { row => { }}}
      }
      ok
    }

    "Select from the ports table" >> {
      time("Ports Query"){ 
      query("""
          SELECT * FROM curePorts;
        """){ _.foreach { row => { }}}
      }
      ok
    }

    "Create the source MV Lens" >> {
      time("Source MV Lens"){
        update("""
          CREATE LENS MV1 
          AS SELECT * FROM cureSource 
          WITH MISSING_VALUE('IMO_CODE');
        """)
      }
      ok
    }

    "Create the locations MV Lens" >> {
      time("Locations MV Lens"){
        update("""
          CREATE LENS MV2 
          AS SELECT * FROM cureLocations 
          WITH MISSING_VALUE('IMO_CODE');
        """)
      }
      ok
    }

    "Explain the CURE Query" >> {
      db.typechecker.schemaOf(select(cureQuery))
      ok
    }
    
     "Run the CURE Query" >> {
       LoggerUtils.trace(
					// "mimir.backend.SparkBackend",
          // "mimir.algebra.Typechecker"
				){ 
          try {
            time("CURE Query"){
              val qr = query(cureQuery){ _.toList.map { row => {1} } }.length
            }
          } catch {
            case mv: MissingVariable => 
              mv.printStackTrace()
              println(mv.context)
              ko
          }
       }
       ok
     }
  }
}
