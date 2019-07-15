package mimir.exec

import java.io._

import org.specs2.mutable._
import org.specs2.specification._

import mimir.util._
import mimir.algebra._
import mimir.test._
import mimir.data.FileFormat

object EvalSpec 
  extends SQLTestSpecification("EvalSpec")
  with BeforeAll
{

  val inventoryDataFile = "test/data/Product_Inventory.csv"

  def beforeAll = {
    db.loader.linkTable(
      inventoryDataFile, 
      FileFormat.CSV,
      ID("PRODUCT_INVENTORY"),
      Map(
        "ignoreLeadingWhiteSpace"->"true",
        "ignoreTrailingWhiteSpace"->"true",
        "DELIMITER" -> ",", 
        "mode" ->"DROPMALFORMED", 
        "header" -> "false"
      )
    )
  }

  "The query evaluator" should {

    "Compute Deterministic Aggregate Queries" >> {
      val q0 = querySingleton("""
        SELECT SUM(QUANTITY)
        FROM PRODUCT_INVENTORY
      """).asLong
      q0 must beEqualTo(92)

      LoggerUtils.debug(
        // "mimir.exec.Compiler"
      ) {query("""
        SELECT COMPANY, SUM(QUANTITY) AS QTY
        FROM PRODUCT_INVENTORY
        GROUP BY COMPANY;
      """){ result =>
        val q1 = result.map { row => 
          (row(ID("COMPANY")).asString, row(ID("QTY")).asLong) 
        }.toSeq
        q1 must have size(3)
        q1 must contain(
          ("Apple", 9), 
          ("HP", 69), 
          ("Sony", 14)
        )

      }}

      query("""
        SELECT COMPANY, MAX(PRICE) AS MP
        FROM PRODUCT_INVENTORY
        GROUP BY COMPANY;
      """) { result => 
        val q2 = result.map { row => 
          (row(ID("COMPANY")).asString, row(ID("MP")).asDouble) 
        }.toSeq
        q2 must have size(3)
        q2 must contain( 
          ("Apple", 13.00), 
          ("HP", 102.74), 
          ("Sony", 38.74) 
        )
      }

      query("""
        SELECT COMPANY, AVG(PRICE) AS AP
        FROM PRODUCT_INVENTORY
        GROUP BY COMPANY;
      """) { result =>
        val q3 = result.map { row => 
          (row(ID("COMPANY")).asString, row(ID("AP")).asDouble) 
        }.toSeq
        q3 must have size(3)
        q3 must contain( 
          ("Apple", 12.5), 
          ("HP", 64.41333333333334), 
          ("Sony", 38.74) 
        )
      }


      query("""
        SELECT COMPANY, MIN(QUANTITY) AS MQ 
        FROM PRODUCT_INVENTORY 
        GROUP BY COMPANY;
      """){ result =>
        val q4 = result.toSeq.map { row => (row(ID("COMPANY")).asString, row(ID("MQ")).asLong) } 
        q4 must have size(3)
        q4 must contain( 
          ("Apple", 4), 
          ("HP", 9), 
          ("Sony", 14) 
        )
      }

      querySingleton("""
        SELECT COUNT(*)
        FROM PRODUCT_INVENTORY;
      """).asLong must beEqualTo(6l)

      querySingleton("""
        SELECT COUNT(DISTINCT COMPANY)
        FROM PRODUCT_INVENTORY;
      """).asLong must beEqualTo(3l)

      querySingleton("""
        SELECT COUNT(*)
        FROM PRODUCT_INVENTORY
        WHERE COMPANY = 'Apple';
      """).asLong must beEqualTo(2l)

      querySingleton("""
        SELECT COUNT(DISTINCT COMPANY)
        FROM PRODUCT_INVENTORY
        WHERE COMPANY = 'Apple';
      """).asLong must beEqualTo(1l)

      query("""
        SELECT P.COMPANY, P.QUANTITY, P.PRICE
        FROM (SELECT COMPANY, MAX(PRICE) AS COST
          FROM PRODUCT_INVENTORY
          GROUP BY COMPANY)subq, PRODUCT_INVENTORY P
        WHERE subq.COMPANY = P.COMPANY AND subq.COST = P.PRICE;
      """){ result => 
        val q8 = result.toSeq.map { row => (
          row(ID("COMPANY")).asString, 
          row(ID("QUANTITY")).asLong, 
          row(ID("PRICE")).asDouble
        )} 
        q8 must have size(3)
        q8 must contain( 
          ("Apple", 5, 13.00), 
          ("HP", 37, 102.74), 
          ("Sony", 14, 38.74) 
        )
      }

      query("""
        SELECT P.COMPANY, P.PRICE
        FROM (SELECT AVG(PRICE) AS A FROM PRODUCT_INVENTORY) subq, PRODUCT_INVENTORY P
        WHERE PRICE > subq.A;
      """) { result =>
        val q9 = result.toSeq.map { row => (
          row(ID("COMPANY")).asString,
          row(ID("PRICE")).asDouble
        )}
        q9 must have size(2)
        q9 must contain( 
          ("HP", 65.00), 
          ("HP", 102.74) 
        )
      }

      query("""
        SELECT MIN(subq2.B)
        FROM (SELECT P.PRICE AS B FROM (SELECT AVG(QUANTITY) AS A FROM PRODUCT_INVENTORY)subq, PRODUCT_INVENTORY P
        WHERE P.QUANTITY > subq.A)subq2;
      """) { result =>
        val q10 = result.toSeq.map { row => row(0).asDouble } 
        q10 must have size(1)
        q10 must contain( 65.00 )
      }
    }

  }

}