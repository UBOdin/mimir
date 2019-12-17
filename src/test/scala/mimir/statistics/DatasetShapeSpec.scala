package mimir.statistics

import org.specs2.mutable._
import org.specs2.specification._
import java.io.File
import mimir.algebra.{Var, StringPrimitive, ID}
import mimir.test._
import mimir.util._

object DatasetShapeSpec
  extends SQLTestSpecification("DatasetShapeSpec")
  with BeforeAll
{

  def beforeAll = {
    db.loader.loadTable(
      targetTable = Some(ID("Z")), 
      sourceFile = "test/r_test/z.csv",
      datasourceErrors = true
    )
    db.loader.loadTable(
      targetTable = Some(ID("Z_BAD")), 
      sourceFile = "test/r_test/z_bad.csv",
      datasourceErrors = true
    )
  }

  "The Dataset Shape Detector" should 
  {
    "Detect the basics" >> {
      val ret = DatasetShape.detect(db, db.table("Z")).map { _.description }
      ret must contain { contain("A, B") }.atLeastOnce // columns
      ret must contain { contain("A should be an int") }.atLeastOnce // columns
    }
    
    "Detect and test the basics with Adaptive Schema and work with DataSource Errors" >> {
      //good data
      db.lenses.create( 
        ID("Z_SW"), 
        ID("SHAPE_WATCHER"),
        db.table("Z"), 
        friendlyName = Some("Z_SHAPE")
      )
      //bad data
      db.lenses.create( 
        ID("Z_BAD_SW"), 
        ID("SHAPE_WATCHER"), 
        db.table("Z_BAD"), 
        config = db.lenses.getConfig(ID("Z_SW")),
        friendlyName = Some("Z_BAD_SHAPE")
      )
      
      db.views.create(ID("Z_BAD_S"), db.lenses.view(ID("Z_BAD_SW")))
      
      val resultSets = 
        LoggerUtils.debug(
          // "mimir.ctables.CTExplainer"
        ) {
          db.uncertainty.explainEverything(db.table("Z_BAD_S"))
        }
      
      resultSets.map(_.all(db).map(_.message)).flatten must contain(eachOf(
          """Missing expected column 'B'""",
          """A had no nulls before, but now has 2""",
          """Unexpected column 'B_0'""",
          """The value [ NULL ] is uncertain because there is an error(s) in the data source on row 911701464 of Z_BAD. The raw value of the row in the data source is [ ,,, ]""",
          """The value [ ---- ] is uncertain because there is an error(s) in the data source on row -2127400930 of Z_BAD. The raw value of the row in the data source is [ ---- ]""",
          """The value [ NULL ] is uncertain because there is an error(s) in the data source on row 911701464 of Z_BAD. The raw value of the row in the data source is [ ,,, ]""",
          """The value [ NULL ] is uncertain because there is an error(s) in the data source on row -2127400930 of Z_BAD. The raw value of the row in the data source is [ ---- ]"""
          ))
      
      val result = query("""
        SELECT * FROM Z_BAD_S
      """)(_.toList.map(_.tuple))
      result must not be empty
    }

  }

}