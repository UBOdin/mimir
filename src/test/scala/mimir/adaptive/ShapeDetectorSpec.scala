package mimir.adaptive

import java.io._
import org.specs2.specification._

import mimir._
import mimir.algebra._
import mimir.test._
import mimir.data._

object ShapeDetectorSpec 
  extends SQLTestSpecification("ShapeDetectorSpec") 
  with BeforeAll
{

  def beforeAll = 
  {
    db.loader.loadTable(
      "test/NYC_CoD/New_York_City_Leading_Causes_of_Death_12_11_2018.csv",
      targetTable = Some(ID("ORIGINAL")),
      format = FileFormat.CSV,
      inferTypes = Some(true),
      detectHeaders = Some(true)
    )
    db.loader.loadTable(
      "test/NYC_CoD/New_York_City_Leading_Causes_of_Death_12_18_2018.csv",
      targetTable = Some(ID("UPDATED")),
      format = FileFormat.CSV,
      inferTypes = Some(true),
      detectHeaders = Some(true)
    )
  }

  sequential

  "Type Inference" should {

    "Be able to create shape detector lenses" >> {
 
      db.adaptiveSchemas.create( 
        ID("CAUSES_ORIGINAL"), 
        ID("SHAPE_WATCHER"), 
        db.table("ORIGINAL"), 
        Seq(StringPrimitive("TEST_MODEL")),
        "Causes"
      )

      ok

    }

    "Be able to process cell-level facets" >> {
 
      db.adaptiveSchemas.create( 
        ID("CAUSES_UPDATED"), 
        ID("SHAPE_WATCHER"), 
        db.table("UPDATED"), 
        Seq(StringPrimitive("TEST_MODEL")),
        "Causes"
      )

      query("SELECT SEX FROM CAUSES_UPDATED") { result => 
        val ret = 
          result.map { row => row(0).asString -> row.isColDeterministic(0) }
                .toIndexedSeq
                .toSet

        ret must not contain( "M" -> false )
        ret must not contain( "F" -> false )
        ret must contain( "M" -> true )
        ret must contain( "F" -> true )

        ret must contain( "Male" -> false )
        ret must contain( "Female" -> false )
        ret must not contain( "Male" -> true )
        ret must not contain( "Female" -> true )
      }

    }

    

  }


}
