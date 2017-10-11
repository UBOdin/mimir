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

  "The Log Loader" should {
    "Load Log Files" >> {
      LoadLog(db, "INVENTORY", new File("test/data/Product_Inventory.sql"));
      querySingleton("""SELECT data FROM inventory WHERE line = 8""").toString must contain("('P123', 'Apple', 4, 12.00)")
    }
  }

  "The File Format Detector" should {

    "Have sensible regexps" >> {

      FileFormat.GZipFileExtension findFirstIn "file.gz" match {
        case Some(_) => ok
        case x => ko(x.toString)
      }

      FileFormat.GZipFileExtension.unapplySeq("file.gz") match {
        case x => ko(x.toString)
      }

      "file.gz" match {
        case FileFormat.GZipFileExtension(c) => ok
        case _ => ko("not matched")
      }
    }

    "Be able to invoke the UNIX `file` utility" >> {
      FileFormat.file("test/data/Product_Inventory.sql").get._1 must be equalTo("ASCII text")
      FileFormat.file("test/data/home.gz").get._1 must be equalTo("gzip compressed data")
    }

    "Recognize common file types" >> {
      FileFormat.ofFile("test/data/Product_Inventory.sql") must be equalTo(FileFormat.Raw)
      FileFormat.ofFile("test/data/home.gz") must be equalTo(FileFormat.GZip)
    }

  }
}