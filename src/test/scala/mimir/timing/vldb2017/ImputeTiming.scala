package mimir.timing.vldb2017

import java.io._
import org.specs2.specification._
import org.specs2.specification.core.Fragments
import org.specs2.concurrent._
import scala.concurrent.duration._


import mimir.algebra._
import mimir.util._
import mimir.ctables.{VGTerm}
import mimir.optimizer.{InlineVGTerms,InlineProjections}
import mimir.test.{SQLTestSpecification, PDBench, TestTimer}
import mimir.models._
import mimir.exec.uncertainty._

object ImputeTiming
  extends VLDB2017TimingTest("VLDB2017Impute", Map("reset" -> "NO", "inline" -> "YES"))//, "initial_db" -> "test/tpch-impute-1g.db"))
  with BeforeAll
{

  sequential

  val fullReset = false
  val runBestGuessQueries = false
  val runTupleBundleQueries = false
  val runSamplerQueries = false

  val timeout = 30.minute

  def beforeAll =
  {
    if(fullReset){
      println("DELETING ALL MIMIR METADATA")
      update("DELETE FROM MIMIR_MODEL_OWNERS")
      update("DELETE FROM MIMIR_MODELS")
      update("DELETE FROM MIMIR_VIEWS")
    }
  }

  val relevantTables = Seq(
    // ("CUSTOMER", Seq("NATIONKEY")),
    // ("LINEITEM", Seq("ORDERKEY", "PARTKEY", "SUPPKEY")),
    // ("PARTSUPP", Seq("PARTKEY", "SUPPKEY")),
    // ("NATION", Seq("REGIONKEY")),
    // ("SUPPLIER", Seq("NATIOKEY")),
    // ("REGION", Seq()),
    ("ORDERS", Seq("CUSTKEY"))
  )

  "TPCH Impute" should {

    sequential
    Fragments.foreach(1 to 1){ i =>

      val TPCHQueries = 
        Seq(
          s"""
            SELECT returnflag, linestatus, 
              SUM(quantity) AS sum_qty,
              SUM(extendedprice) AS sum_base_price,
              SUM(extendedprice * (1-discount)) AS sum_disc_price,
              SUM(extendedprice * (1-discount)*(1+tax)) AS sum_charge,
              AVG(quantity) AS avg_qty,
              AVG(extendedprice) AS avg_price,
              AVG(discount) AS avg_disc,
              COUNT(*) AS count_order
            FROM lineitem_run_$i
            WHERE shipdate <= DATE('1997-09-01')
            GROUP BY returnflag, linestatus;  
          """,
          s"""
            SELECT o.orderkey, 
                   o.orderdate,
                   o.shippriority,
                   SUM(extendedprice * (1 - discount)) AS query3
            FROM   customer_run_$i c, orders_run_$i o, lineitem_run_$i l
            WHERE  c.mktsegment = 'BUILDING'
              AND  o.custkey = c.custkey
              AND  l.orderkey = o.orderkey
              AND  o.orderdate < DATE('1995-03-15')
              AND  l.shipdate > DATE('1995-03-15')
            GROUP BY o.orderkey, o.orderdate, o.shippriority;
          """,
          s"""
            SELECT n.name, SUM(l.extendedprice * (1 - l.discount)) AS revenue 
            FROM   customer_run_$i c, orders_run_$i o, lineitem_run_$i l, supplier_run_$i s, nation_run_$i n, region_run_$i r
            WHERE  c.custkey = o.custkey
              AND  l.orderkey = o.orderkey 
              AND  l.suppkey = s.suppkey
              AND  c.nationkey = s.nationkey 
              AND  s.nationkey = n.nationkey 
              AND  n.regionkey = r.regionkey 
              AND  r.name = 'ASIA'
              AND  o.orderdate >= DATE('1994-01-01')
              AND  o.orderdate <  DATE('1995-01-01')
            GROUP BY n.name
          """,
          s"""
            SELECT nation, o_year, SUM(amount) AS sum_profit 
            FROM (
              SELECT n.name AS nation, 
                     EXTRACT(year from o.orderdate) AS o_year,
                     ((l.extendedprice * (1 - l.discount)) - (ps.supplycost * l.quantity))
                        AS amount
              FROM   part_run_$i p, supplier_run_$i s, lineitem_run_$i l, partsupp_run_$i ps, orders_run_$i o, nation_run_$i n
              WHERE  s.suppkey = l.suppkey
                AND  ps.suppkey = l.suppkey 
                AND  ps.partkey = l.partkey
                AND  p.partkey = l.partkey
                AND  o.orderkey = l.orderkey 
                AND  s.nationkey = n.nationkey 
                AND  (p.name LIKE '%green%')
              ) AS profit 
            GROUP BY nation, o_year;
          """
        )


      sequential

      // CREATE LENSES
      Fragments.foreach(
        relevantTables.toSeq
      ){ createMissingValueLens(_, s"_run_$i") }

      // QUERIES
      Fragments.foreach( 
        if(!runBestGuessQueries){ Seq() } 
        else { TPCHQueries.map { (_, 60.0) }.zipWithIndex }
      ) {
        queryLens(_)
      }

      Fragments.foreach(
        if(!runTupleBundleQueries){ Seq() } 
        else { TPCHQueries.zipWithIndex }
      ){
        sampleFromLens(_)
      }

      Fragments.foreach(
        if(!runSamplerQueries){ Seq() } 
        else { TPCHQueries.zipWithIndex }
      ){
        expectedFromLens(_)
      }


    }

  }
}
