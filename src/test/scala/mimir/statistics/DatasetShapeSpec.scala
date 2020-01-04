package mimir.statistics

import org.specs2.mutable._
import org.specs2.specification._
import java.io.File
import mimir.algebra._
import mimir.test._
import mimir.util._
import mimir.statistics.facet._

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
    db.loader.loadTable(
      targetTable = Some(ID("COD_A")),
      sourceFile = "test/NYC_CoD/New_York_City_Leading_Causes_of_Death_12_11_2018.csv",
      datasourceErrors = true
    )
    db.loader.loadTable(
      targetTable = Some(ID("COD_B")),
      sourceFile = "test/NYC_CoD/New_York_City_Leading_Causes_of_Death_12_18_2018.csv",
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
      db.adaptiveSchemas.create( 
        ID("Z_SW"), 
        ID("SHAPE_WATCHER"),
        db.table("Z"), 
        Seq(StringPrimitive("MIMIR_SHAPE_Z")), 
        "Z_SHAPE"
      )
      //bad data
      db.adaptiveSchemas.create( 
        ID("Z_BAD_SW"), 
        ID("SHAPE_WATCHER"), 
        db.table("Z_BAD"), 
        Seq(Var(ID("MIMIR_SHAPE_Z"))), 
        "Z_BAD_SHAPE"
      )
      
      db.views.create(ID("Z_BAD_S"), db.adaptiveSchemas.viewFor(ID("Z_BAD_SW"), ID("Z_BAD_SW")).get)
      
      val resultSets = 
        LoggerUtils.debug(
          // "mimir.ctables.CTExplainer"
        ) {
          db.uncertainty.explainEverything(db.table("Z_BAD_S"))
        }
      
      resultSets.map(_.all(db).map(_.toJSON)).flatten must contain(eachOf(
          """{"rowidarg":0,"source":"MIMIR_DSE_WARNING_Z_BAD_DSE","confirmed":false,"varid":0,"english":"There is an error(s) in the data source on row 911701464 of Z_BAD. The raw value of the row in the data source is [ ,,, ]","repair":{"selector":"warning"},"args":["'911701464'"]}""",
          """{"rowidarg":-1,"source":"MIMIR_TI_ATTR_Z_BAD_TI","confirmed":true,"varid":0,"english":"I guessed that Z_BAD.B_0 was of type INT because all of the data fit","repair":{"selector":"list","values":[{"choice":"real","weight":0.8},{"choice":"int","weight":0.8},{"choice":"varchar","weight":0.5}]},"args":[1]}""",
          """{"rowidarg":3,"source":"MIMIR_TI_WARNING_Z_BAD_TI","confirmed":false,"varid":0,"english":"Couldn't Cast [ ---- ] to int on row -2127400930 of Z_BAD.A","repair":{"selector":"warning"},"args":["'A'","'----'","'int'","'-2127400930'"]}""",
          """{"rowidarg":-1,"source":"MIMIR_TI_ATTR_Z_BAD_TI","confirmed":false,"varid":0,"english":"I guessed that Z_BAD.A was of type INT because around 80% of the data fit","repair":{"selector":"list","values":[{"choice":"real","weight":0.8},{"choice":"int","weight":0.8},{"choice":"varchar","weight":0.5}]},"args":[0]}""",
          """{"rowidarg":0,"source":"MIMIR_DSE_WARNING_Z_BAD_DSE","confirmed":false,"varid":0,"english":"There is an error(s) in the data source on row -2127400930 of Z_BAD. The raw value of the row in the data source is [ ---- ]","repair":{"selector":"warning"},"args":["'-2127400930'"]}""",
          """{"rowidarg":-1,"source":"MIMIR_CH_Z_BAD_DH","confirmed":false,"varid":0,"english":"I analyzed the first several rows of Z_BAD and there appear to be column headers in the first row.  For column with index: 1, the detected header is B_0","repair":{"selector":"by_type","type":"varchar"},"args":[1]}""",
          """{"rowidarg":-1,"source":"MIMIR_CH_Z_BAD_DH","confirmed":false,"varid":0,"english":"I analyzed the first several rows of Z_BAD and there appear to be column headers in the first row.  For column with index: 0, the detected header is A","repair":{"selector":"by_type","type":"varchar"},"args":[0]}"""
          ))
      
      val result = query("""
        SELECT * FROM Z_BAD_S
      """)(_.toList.map(_.tuple))
      result must not be empty
    }

    "Detect Nulls and Ranges Correctly" >> {

      val retA = DatasetShape.detect(db, db.table("COD_A"))
      retA.map { _.description } must contain("DEATH_RATE has no more than 35.2% nulls")
      retA must contain(new NonNullable(ID("YEAR")))
      retA must contain(new DrawnFromDomain(ID("SEX"), TString(), Set(
        StringPrimitive("M"),
        StringPrimitive("F")
      )))

      // DEATHS should, in principle also contain nulls, but Spark is happy to cast '.' as a 0.

      val retB = DatasetShape.detect(db, db.table("COD_B"))
      retB.map { _.description } must not contain("DEATHS has no nulls")  // 2015, 2016 data uses ' ' for this column.
      retB must contain(
        new DrawnFromDomain(ID("SEX"), TString(), Set(
          StringPrimitive("Female"),
          StringPrimitive("Male"),
          StringPrimitive("M"),
          StringPrimitive("F")          
        ))
      )

    }

  }

}