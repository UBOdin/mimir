package mimir.load

import java.io._

import org.specs2.specification._
import org.specs2.mutable._

import mimir.algebra._
import mimir.test._
import mimir.util._

object LoadLogSpec
  extends SQLTestSpecification("LoadLog")
{

  "The File Format Detector" should {
    "Be able to invoke the UNIX `file` utility" >> {
      FileFormat.inspectFile("test/data/Product_Inventory.sql").get._1 must be equalTo("ASCII text")
      FileFormat.inspectFile("test/data/home.gz").get._1 must be equalTo("gzip compressed data")
    }
    "Recognize common file types" >> {
      FileFormat.ofFile("test/data/Product_Inventory.sql") must be equalTo(FileFormat.Raw)
      FileFormat.ofFile("test/data/home.gz") must be equalTo(FileFormat.GZip)
    }
  }

  "The Log Loader" should {
    "Load Log Files" >> {
      LoadLog(db, "INVENTORY", new File("test/data/Product_Inventory.sql"))
      querySingleton("""
        SELECT data FROM inventory WHERE line = 8
      """).toString must contain("('P123', 'Apple', 4, 12.00)")
    }
    "Load GZipped Log Files" >> {
      LoadLog(db, "HOME", new File("test/data/home.gz"))
      querySingleton("""
        SELECT data FROM home WHERE line = 1
      """).toString must contain("time")
    }
  }

}