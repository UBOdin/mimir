package mimir.exec

import java.io._

import org.specs2.mutable._
import org.specs2.specification._

import mimir.util._
import mimir.algebra._
import mimir.test._

object EvalSpec 
  extends SQLTestSpecification("EvalSpec")
  with BeforeAll
{

  val inventoryDataFile = new File("test/data/Product_Inventory.sql")

  def beforeAll = {
    stmts(inventoryDataFile).map( update(_) )
  }

  "The query evaluator" should {

    "Compute Deterministic Aggregate Queries" >> {
      val q0 = query("""
        SELECT SUM(QUANTITY)
        FROM PRODUCT_INVENTORY
      """).allRows.flatten
      q0 must have size(1)
      q0 must contain(i(92))

      val q1 = 
        LoggerUtils.debug(List(
          // "mimir.exec.Compiler"
        ), () => {
          query("""
            SELECT COMPANY, SUM(QUANTITY)
            FROM PRODUCT_INVENTORY
            GROUP BY COMPANY;
          """)
        }).allRows.flatten
      q1 must have size(6)
      q1 must contain( str("Apple"), i(9), str("HP"), i(69), str("Sony"), i(14) )

      val q2 = query("""
        SELECT COMPANY, MAX(PRICE)
        FROM PRODUCT_INVENTORY
        GROUP BY COMPANY;
                                                  """).allRows.flatten
      q2 must have size(6)
      q2 must contain( str("Apple"), f(13.00), str("HP"), f(102.74), str("Sony"), f(38.74) )

      val q3 = query("""
        SELECT COMPANY, AVG(PRICE)
        FROM PRODUCT_INVENTORY
        GROUP BY COMPANY;
                                                  """).allRows.flatten

      q3 must have size(6)
      q3 must contain( str("Apple"), f(12.5), str("HP"), f(64.41333333333334), str("Sony"), f(38.74) )

      val q4 = query("""SELECT COMPANY, MIN(QUANTITY)FROM PRODUCT_INVENTORY GROUP BY COMPANY;""").allRows.flatten
      q4 must have size(6)
      q4 must contain( str("Apple"), i(4), str("HP"), i(9), str("Sony"), i(14) )

      val q5 = query("""
        SELECT COUNT(*)
        FROM PRODUCT_INVENTORY;
      """).allRows.flatten
      q5 must have size(1)
      q5 must contain( i(6) )

      val q6 = query("""
        SELECT COUNT(DISTINCT COMPANY)
        FROM PRODUCT_INVENTORY;
      """).allRows.flatten
      q6 must have size(1)
      q6 must contain( i(3) )

      val q7a = query("""
        SELECT COUNT(*)
        FROM PRODUCT_INVENTORY
        WHERE COMPANY = 'Apple';
      """).allRows.flatten
      q7a must have size(1)
      q7a must contain( i(2) )

      val q7b = query("""
        SELECT COUNT(DISTINCT COMPANY)
        FROM PRODUCT_INVENTORY
        WHERE COMPANY = 'Apple';
      """).allRows.flatten
      q7b must have size(1)
      q7b must contain( i(1) )

      val q8 = query("""
        SELECT P.COMPANY, P.QUANTITY, P.PRICE
        FROM (SELECT COMPANY, MAX(PRICE) AS COST
          FROM PRODUCT_INVENTORY
          GROUP BY COMPANY)subq, PRODUCT_INVENTORY P
        WHERE subq.COMPANY = P.COMPANY AND subq.COST = P.PRICE;
                                                                                                              """).allRows.flatten
      q8 must have size(9)
      q8 must contain( str("Apple"), i(5), f(13.00), str("HP"), i(37), f(102.74), str("Sony"), i(14), f(38.74) )

      val q9 = query("""
        SELECT P.COMPANY, P.PRICE
        FROM (SELECT AVG(PRICE) AS A FROM PRODUCT_INVENTORY)subq, PRODUCT_INVENTORY P
        WHERE PRICE > subq.A;
                                                                                                                                  """).allRows.flatten
      q9 must have size(4)
      q9 must contain( str("HP"), f(65.00), str("HP"), f(102.74) )

      val q10 = query("""
        SELECT MIN(subq2.B)
        FROM (SELECT P.PRICE AS B FROM (SELECT AVG(QUANTITY) AS A FROM PRODUCT_INVENTORY)subq, PRODUCT_INVENTORY P
        WHERE P.QUANTITY > subq.A)subq2;
                                                    """).allRows.flatten
      q10 must have size(1)
      q10 must contain( f(65.00) )
    }

  }

}