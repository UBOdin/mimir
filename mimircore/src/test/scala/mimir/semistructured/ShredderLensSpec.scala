package mimir.semistructured

import java.io._;

import mimir._;
import mimir.sql._;
import mimir.algebra._;
import mimir.ctables._;
import mimir.test._;
import mimir.util._;

object ShredderLensSpec
  extends SQLTestSpecification("shredderLensTestDB")
{

  val testTable = "JSONDATA"
  val discala = new FuncDep()
  val testData = new File(
    //"../test/data/JSONOUTPUTWIDE.csv"
    "../test/data/Bestbuy_raw_noquote.csv"
  )
  val extractorName = "TEST_EXTRACTOR"

  sequential

  "The DiScala Extractor" should {

    "be initializable" >> {
      LoadCSV.handleLoadTable(db, testTable, testData)
      val schema = db.getTableSchema(testTable).get
      discala.initialStep(schema, db.query(db.getTableOperator(testTable)))
      discala.graphPairs must not beNull
    }

    "be serializable" >> {
      discala.serializeTo(db, extractorName) 
      discala.graphPairs must not beNull

      val blob1 = 
        db.backend.singletonQuery(
          "SELECT data FROM "+FuncDep.BACKSTORE_TABLE_NAME+" WHERE name='"+extractorName+"'"
        )
      blob1 must beAnInstanceOf[BlobPrimitive]
      blob1.asInstanceOf[BlobPrimitive].v.length must be greaterThan(0)
    }

    "be deserializable" >> {
      val temp = FuncDep.deserialize(db, extractorName)
      temp.graphPairs must not beNull
    }



  }

}
