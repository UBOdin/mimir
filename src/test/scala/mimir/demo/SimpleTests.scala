package mimir.demo

import java.io.File

import mimir.Database
import mimir.Mimir.{conf, db}
import mimir.algebra.{PrimitiveValue, StringPrimitive, TFloat}
import org.specs2.matcher.FileMatchers
import mimir.test._
import mimir.lenses.JsonExplorerLens
import mimir.sql.JDBCBackend

object SimpleTests
  extends SQLTestSpecification("tempDBDemoScript")
    with FileMatchers
{

  // The demo spec uses cumulative tests --- Each stage depends on the stages that
  // precede it.  The 'sequential' keyword below is necessary to prevent Specs2 from
  // automatically parallelizing testing.
  sequential

  val database = new Database(new JDBCBackend("sqlite", "test/json.db"))
  database.backend.open()
  database.initializeDBForMimir();

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

    "Run the Load Product Data Script" >> {
      stmts(productDataFile).map( update(_) )
      db.backend.resultRows("SELECT * FROM PRODUCT;") must have size(6)
    }

    "Load CSV Files" >> {

      // load all tables without TI lens
//      reviewDataFiles.foreach(db.loadTableNoTI(_))

      db.loadTable(new File("test/data/ratings1.csv"))
      query("SELECT * FROM RATINGS1;"){
        _.map{
          _(0)
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
