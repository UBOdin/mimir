package mimir.demo

import java.io._
import org.specs2.reporter.LineLogger
import org.specs2.specification.core.{Fragment,Fragments}

import mimir.test._
import mimir.util._

object CureScenario
  extends SQLTestSpecification("CureScenario")
{

  val dataFiles = List(
    new File("test/data/cureSource.csv"),
    new File("test/data/cureLocations.csv")
  )

  def time[A](description: String, op: () => A): A = {
    val t:StringBuilder = new StringBuilder()
    TimeUtils.monitor(description, op, println(_)) 
  }

  sequential 

  "The CURE Scenario" should {
    Fragment.foreach(dataFiles){ table => {
      s"Load '$table'" >> {
        time(
          s"Load '$table'",
          () => { db.loadTable(table) }
        )
        ok
      }
    }}

    // "CURE Timing Tests" >> {

    //   time("Type Inference Query", 
    //     () => {
    //       query("""
    //         SELECT * FROM cureSource;
    //       """).foreachRow((x) => {})
    //     }
    //   )

    //   time("Source MV Lens",
    //     () => {
    //       update("""
    //         CREATE LENS MV1 
    //         AS SELECT * FROM cureSource 
    //         WITH MISSING_VALUE('IMO_CODE');
    //       """)
    //     }
    //   )

    //   time("Locations MV Lens",
    //     () => {
    //       update("""
    //         CREATE LENS MV2 
    //         AS SELECT * FROM cureLocations 
    //         WITH MISSING_VALUE('IMO_CODE');
    //       """)
    //     }
    //   )

  //     TimeUtils.monitor("CURE Query",
  //       () => {
  //         query("""
  //           SELECT * 
  //           FROM   MV1 AS source 
  //             JOIN MV2 AS locations 
  //                     ON source.IMO_CODE = locations.IMO_CODE;
  //         """).foreachRow((x) => {})
  //       },
  //       logger.info(_)
  //     )

  //     ok
    // }    
  }
}
