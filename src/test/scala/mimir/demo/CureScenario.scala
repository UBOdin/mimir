package mimir.demo

import java.io._
import org.specs2.reporter.LineLogger
import org.specs2.specification.core.{Fragment,Fragments}

import mimir.test._
import mimir.util._
import LoggerUtils.trace
import mimir.ctables._

object CureScenario
  extends SQLTestSpecification("CureScenario",  Map("reset" -> "YES"))
{

  val dataFiles = List(
    new File("test/data/cureSource.csv"),
    new File("test/data/cureLocations.csv"),
    new File("test/data/curePorts.csv")
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
    FROM CURESOURCE AS SRC
      JOIN CURELOCATIONS AS LOC ON SRC.IMO_CODE = LOC.IMO_CODE
        LEFT OUTER JOIN CUREPORTS AS PORTS ON SRC.PORT_OF_ARRIVAL = PORTS.PORT
        WHERE SPEED(DST(LOC.LON, LOC.LAT, PORTS.LON, PORTS.LAT),SRC.DATE, DATE('2016-09-03')) > 100;
    ;"""

  def time[A](description: String): ( => A) => A =
    Timer.monitor(description)

  sequential 

  "The CURE Scenario" should {
    Fragment.foreach(dataFiles){ table => {
      val basename = table.getName().replace(".csv", "").toUpperCase
      s"Load '$table'" >> {
        time(s"Load '$table'") {
          update(s"LOAD '$table';") 
        }
        time(s"Materialize '$basename'"){
          //XXX: there is a problem with materialized views and MV lens
          //      that needs to be addressed more thoroughly - 
          //      something with col det bit vector and taint cols
          //      it looks like taint cols for prov (MIMIR_COL_DET_MIMIR_ROWID_X) 
          //      are in the non-materialized VIEW's TAINT metadata
          //      but not in the materialized version of the same view.
          //      I added a hack in CTPercolator (see line 530)
          //      but is likely the wrong solution
          //     -Mike
          update(s"ALTER VIEW $basename MATERIALIZE;")
        }
        db.explainer.explainEverything(
          db.sql.convert(stmt(s"SELECT * FROM $basename")
            .asInstanceOf[net.sf.jsqlparser.statement.select.Select])) must not beEmpty;
        //this still blows up - something with getColumns on vgterm during lookup query 
        /*db.explainer.explainEverything(
          db.sql.convert(stmt(s"SELECT * FROM $basename")
            .asInstanceOf[net.sf.jsqlparser.statement.select.Select]))
              .flatMap( rs => rs.all(db)) must not beEmpty;*/
        
        db.tableExists(basename) must beTrue
      }
    }}

    "Select from the source table" >> {
      time("Type Inference Query"){ 
        query("""
          SELECT * FROM cureSource;
        """){ _.foreach { row => {} } }
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

//    time("Materalize MV1", () => {db.selectInto("MAT_MV1","MV1")})
//    time("Materalize MV2", () => {db.selectInto("MAT_MV2","MV2")})

/*
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
*/

//    true
    "Explain the CURE Query" >> {
      db.schemaOf(select(cureQuery))
      ok
    }
     "Run the CURE Query" >> {
       time("CURE Query"){
         query(cureQuery){ _.foreach { row => {} } }
       }
//         failed type detection --> run type inferencing
//         --> repair with repairing tool
       ok
     }

     /*"Test Prioritizer" >> {


       update("CREATE TABLE R(A string, B int, C int)")
       loadCSV("R", new File("test/r_test/r.csv"))
       update("CREATE LENS TI AS SELECT * FROM R WITH TYPE_INFERENCE(0.5)")
       update("CREATE LENS MV AS SELECT * FROM TI WITH MISSING_VALUE('B', 'C')")
       val reasonsets1 = explainEverything("SELECT * FROM MV").flatMap(x=>x.all(db))

       reasonsets1 must not beEmpty;

       //CTPrioritizer.prioritize(reasonsets1)

       val reasonsets2 = explainEverything("""
         SELECT
                 BILL_OF_LADING_NBR,
                 SRC.IMO_CODE           AS "SRC_IMO",
                 LOC.LAT                AS "VESSEL_LAT",
                 LOC.LON                AS "VESSEL_LON",
                 PORTS.LAT              AS "PORT_LAT",
                 PORTS.LON              AS "PORT_LON",
                 DATE(SRC.DATE)          AS "SRC_DATE",
                 DST(LOC.LON, LOC.LAT, PORTS.LON, PORTS.LAT) AS "DISTANCE",  SPEED(DST(LOC.LON, LOC.LAT, PORTS.LON, PORTS.LAT), SRC.DATE, NULL) AS "SPEED"
               FROM CURESOURCE AS SRC
                JOIN CURELOCATIONS AS LOC ON SRC.IMO_CODE = LOC.IMO_CODE
                 LEFT OUTER JOIN CUREPORTS AS PORTS ON SRC.PORT_OF_ARRIVAL = PORTS.PORT
                 WHERE SPEED(DST(LOC.LON, LOC.LAT, PORTS.LON, PORTS.LAT),SRC.DATE, NULL) > 100;
       """).flatMap(x=>x.all(db))

       reasonsets2 must not beEmpty;

       CTPrioritizer.prioritize(reasonsets2)
       ok
     }*/

    /*
SELECT
  BILL_OF_LADING_NBR,
  SRC.IMO_CODE           AS "SRC_IMO",
  LOC.LAT                AS "VESSEL_LAT",
  LOC.LON                AS "VESSEL_LON",
  PORTS.LAT              AS "PORT_LAT",
  PORTS.LON              AS "PORT_LON",
  DATE('now')            AS "NOW",
  SRC.DATE,
  DATE(SRC.DATE)          AS "SRC_DATE",
  (julianday(DATE('now'))-julianday(DATE(SRC.DATE)))      AS "TIME_DIFF",
  ABS(LOC.LAT - PORTS.LAT)                  AS "DISTANCE"
FROM CURESOURCE_RAW AS SRC
  JOIN CURELOCATIONS_RAW AS LOC ON SRC.IMO_CODE = LOC.IMO_CODE
  LEFT OUTER JOIN CUREPORTS_RAW AS PORTS ON SRC.PORT_OF_ARRIVAL = PORTS.PORT
LIMIT 1;


SELECT
  BILL_OF_LADING_NBR,
  SRC.IMO_CODE           AS "SRC_IMO",
  LOC.LAT                AS "VESSEL_LAT",
  LOC.LON                AS "VESSEL_LON",
  PORTS.LAT              AS "PORT_LAT",
  PORTS.LON              AS "PORT_LON",
  DATE('now')            AS "NOW",
  SRC.DATE,
  DATE(SRC.DATE)          AS "SRC_DATE",
  (julianday(DATE('now'))-julianday(DATE(SRC.DATE)))      AS "TIME_DIFF",
  ABS(LOC.LAT - PORTS.LAT)                  AS "DISTANCE"
FROM CURESOURCE_RAW AS SRC
  JOIN CURELOCATIONS_RAW AS LOC ON SRC.IMO_CODE = LOC.IMO_CODE
  LEFT OUTER JOIN CUREPORTS_RAW AS PORTS ON SRC.PORT_OF_ARRIVAL = PORTS.PORT
LIMIT 1;


SELECT
  BILL_OF_LADING_NBR,
  SRC.IMO_CODE           AS "SRC_IMO",
  LOC.LAT                AS "VESSEL_LAT",
  LOC.LON                AS "VESSEL_LON",
  PORTS.LAT              AS "PORT_LAT",
  PORTS.LON              AS "PORT_LON",
  DATE('now')            AS "NOW",
  SRC.DATE,
  DATE(SRC.DATE)          AS "SRC_DATE",
  MINUS(julianday(DATE('now')), julianday(DATE(SRC.DATE)))      AS "TIME_DIFF",
  MINUS(LOC.LAT, PORTS.LAT)                  AS "DISTANCE"
FROM CURESOURCE_RAW AS SRC
  JOIN CURELOCATIONS_RAW AS LOC ON SRC.IMO_CODE = LOC.IMO_CODE
  LEFT OUTER JOIN CUREPORTS_RAW AS PORTS ON SRC.PORT_OF_ARRIVAL = PORTS.PORT
LIMIT 1;


SELECT
  BILL_OF_LADING_NBR,
  SRC.IMO_CODE           AS "SRC_IMO",
  LOC.LAT                AS "VESSEL_LAT",
  LOC.LON                AS "VESSEL_LON",
  PORTS.LAT              AS "PORT_LAT",
  PORTS.LON              AS "PORT_LON",
  DATE('now')            AS "NOW",
  SRC.DATE,
  DATE(SRC.DATE)          AS "SRC_DATE",
  MINUS(DATE('now'), DATE(SRC.DATE))      AS "TIME_DIFF",
  MINUS(LOC.LAT, PORTS.LAT)                  AS "DISTANCE"
FROM CURESOURCE_RAW AS SRC
  JOIN CURELOCATIONS_RAW AS LOC ON SRC.IMO_CODE = LOC.IMO_CODE
  LEFT OUTER JOIN CUREPORTS_RAW AS PORTS ON SRC.PORT_OF_ARRIVAL = PORTS.PORT
LIMIT 1;



SELECT DISTINCT
  BILL_OF_LADING_NBR,
  SRC.IMO_CODE           AS "SRC_IMO",
  LOC.LAT                AS "VESSEL_LAT",
  LOC.LON                AS "VESSEL_LON",
  PORTS.LAT              AS "PORT_LAT",
  PORTS.LON              AS "PORT_LON",
  DATE(SRC.DATE)          AS "SRC_DATE",
  DST(LOC.LAT, LOC.LON, PORTS.LAT, PORTS.LON) AS "DISTANCE",  SPEED(DST(LOC.LAT, LOC.LON, PORTS.LAT, PORTS.LON), SRC.DATE, NULL) AS "SPEED"
FROM CURESOURCE_RAW AS SRC
  JOIN CURELOCATIONS_RAW AS LOC ON SRC.IMO_CODE = LOC.IMO_CODE
  LEFT OUTER JOIN CUREPORTS_RAW AS PORTS ON SRC.PORT_OF_ARRIVAL = PORTS.PORT
LIMIT 100;
     */
  }
}
