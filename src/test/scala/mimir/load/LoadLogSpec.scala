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

}