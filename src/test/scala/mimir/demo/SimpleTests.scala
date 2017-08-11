package mimir.demo

import java.io.File

import mimir.Database
import mimir.Mimir.{conf, db}
import mimir.algebra.{PrimitiveValue, StringPrimitive, TFloat}
import org.specs2.matcher.FileMatchers
import mimir.test._
import mimir.lenses.JsonExplorerLens
import mimir.sql.JDBCBackend
import mimir.util.TimeUtils

object SimpleTests
  extends SQLTestSpecification("tempDBDemoScript")
    with FileMatchers
{


  def time[A](description: String): ( => A) => A =
    TimeUtils.monitor(description)

  // The demo spec uses cumulative tests --- Each stage depends on the stages that
  // precede it.  The 'sequential' keyword below is necessary to prevent Specs2 from
  // automatically parallelizing testing.
  sequential

  val jsonDB = new Database(new JDBCBackend("sqlite", "test/json.db"))
  jsonDB.backend.open()
  jsonDB.initializeDBForMimir();

  val debugDB = new Database(new JDBCBackend("sqlite", "databases/debug.db"))
  debugDB.backend.open()
  debugDB.initializeDBForMimir();

  val productDataFile = new File("test/data/Product.sql");
  val reviewDataFiles = List(
    new File("test/data/ratings1.csv"),
    new File("test/data/jsonTest.csv")
  )

  "The Basic Demo" should {
    "Be able to open the database" >> {
      db // force the DB to be loaded
      dbFile must beAFile
    }

    "Load CSV Files" >> {

      // load all tables without TI lens
      //reviewDataFiles.foreach(db.loadTableNoTI(_))
//      db.loadTable("TWITTER",new File("test/data/twitter10kRows.txt"),false,("JSON",Seq(new StringPrimitive(""))))
      db.loadTable("JSONTEST",new File("test/data/jsonTest.csv"),false,("JSON",Seq(new StringPrimitive(""))))
      db.loadTable("JSONTEST2",new File("test/data/meteorite.json"),false,("JSON",Seq(new StringPrimitive(""))))
//      db.loadTable("JSONTEST3",new File("test/data/nasa.json"),false,("JSON",Seq(new StringPrimitive(""))))
      db.loadTable("JSONTEST4",new File("test/data/jeopardy.json"),false,("JSON",Seq(new StringPrimitive(""))))

      println("DONE LOADING")
/*

      query("SELECT JSON_EXPLORER_PROJECT(JSONCOLUMN) FROM JSONTEST;"){
        _.map{
          _(0)
        }
      }

      query("SELECT JSON_EXPLORER_MERGE(JSON_EXPLORER_PROJECT(JSONCOLUMN)) FROM JSONTEST;"){
        _.map{
          _(0)
        }
      }
*/
/*
      query("SELECT * FROM JSONTEST4;"){
        _.foreach(println(_))
      }
*/

    time("Query Time") {
      query("SELECT JSON_EXPLORER_MERGE(JSON_EXPLORER_PROJECT(JSONCOLUMN)) FROM JSONTEST4;") {
        _.map {
          _ (0)
        }
      }
    }

      /*
            // Test Create Type Inference
            val createTI: String = """CREATE LENS TI1 as SELECT * FROM RATINGS1 WITH TYPE_INFERENCE(.8);"""
            update(createTI)

            // Try and select from TI lens
            query("SELECT * FROM TI1;"){
              _.map{
                _(0)
              }
            }
      */
      /*
            // JSON EXPLORER Query

            val JsonQuery =  "SELECT * FROM JSONDATA;"
            query("SELECT * FROM JSONTEST;"){
              _.map{
                _(0)
              }
            }

            val explorer = JsonExplorerLens
            explorer.create(database,"TEST1",mimir.util.SqlUtils.plainSelectStringtoOperator(database,JsonQuery), List(StringPrimitive("JSONCOLUMN").asInstanceOf[PrimitiveValue]))
      */
      /*
            query("SELECT RATING FROM RATINGS1_RAW;") {
              _.map {
                _ (0)
              }.toSeq must contain(str("4.5"), str("A3"), str("4.0"), str("6.4"))
            }

            val result = query("SELECT * FROM RATINGS1.RATINGS1;") {
              _.toSeq
            }
      */
      true
    }

  }
}