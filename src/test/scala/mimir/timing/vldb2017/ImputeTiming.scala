package mimir.timing.vldb2017

import java.io._
import org.specs2.specification._
import org.specs2.specification.core.Fragments
import org.specs2.concurrent._
import scala.concurrent.duration._


import mimir.algebra._
import mimir.util._
import mimir.ctables.InlineVGTerms
import mimir.optimizer.operator.InlineProjections
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
  val runSamplerQueries = true
  val useMaterialized = false
  val useFastPathCache = true
  val skipImputeTests = false
  
  val timeout = 15.minute

  def beforeAll =
  {
    if(fullReset && ! skipImputeTests){
      println("DELETING ALL MIMIR METADATA")
      update("DELETE FROM MIMIR_MODEL_OWNERS")
      update("DELETE FROM MIMIR_MODELS")
      update("DELETE FROM MIMIR_VIEWS")
    }
  }

  val relevantTables = Seq(
    ("CUSTOMER", Seq("NATIONKEY")),
    ("LINEITEM", Seq("ORDERKEY", "PARTKEY", "SUPPKEY")),
    ("PARTSUPP", Seq("PARTKEY", "SUPPKEY")),
    ("NATION", Seq("REGIONKEY")),
    ("SUPPLIER", Seq("NATIONKEY")),
    ("ORDERS", Seq("CUSTKEY"))
  )

  val relevantIndexes = Seq(
    ("SUPPLIER", Seq(
      Seq("SUPPKEY"),
      Seq("NATIONKEY")
    )),
    ("PARTSUPP", Seq(
      Seq("PARTKEY", "SUPPKEY"),
      Seq("SUPPKEY")
    )),
    ("CUSTOMER", Seq(
      Seq("CUSTKEY"),
      Seq("NATIONKEY")
    )),
    ("NATION", Seq(
      Seq("NATIONKEY")
    )),
    ("LINEITEM", Seq(
      Seq("ORDERKEY", "LINENUMBER"),
      Seq("PARTKEY"),
      Seq("SUPPKEY")
    )),
    ("ORDERS", Seq(
      Seq("ORDERKEY"),
      Seq("CUSTKEY")
    ))
  )
  if(skipImputeTests){ "Skipping TPCH Inpute Test" >> ok } else {
    "TPCH Impute" should {

      sequential
      Fragments.foreach(1 to 1){ i =>

        val TPCHQueries = 
          Seq(
            //
            s"""
              -- TPC-H Query 1
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
            """
            // ,
            // s"""
            //   -- TPC-H Query 3
            //   SELECT o.orderkey, 
            //          o.orderdate,
            //          o.shippriority,
            //          SUM(extendedprice * (1 - discount)) AS query3
            //   FROM   customer_run_$i c, orders_run_$i o, lineitem_run_$i l
            //   WHERE  c.mktsegment = 'BUILDING'
            //     AND  o.custkey = c.custkey
            //     AND  l.orderkey = o.orderkey
            //     AND  o.orderdate < DATE('1995-03-15')
            //     AND  l.shipdate > DATE('1995-03-15')
            //   GROUP BY o.orderkey, o.orderdate, o.shippriority;
            // """
            // ,
            // s"""
            //   -- TPC-H Query 5 - NoAgg
            //   SELECT n.name, l.extendedprice * (1 - l.discount) AS revenue 
            //    FROM   region r, nation_run_$i n, supplier_run_$i s, customer_run_$i c, orders_run_$i o, lineitem_run_$i l 
            //   WHERE  r.name = 'ASIA'
            //     AND  n.regionkey = r.regionkey 
            //     AND  c.custkey = o.custkey
            //     AND  o.orderdate >= DATE('1994-01-01')
            //     AND  o.orderdate <  DATE('1995-01-01')
            //     AND  l.orderkey = o.orderkey 
            //     AND  l.suppkey = s.suppkey
            //     AND  c.nationkey = s.nationkey 
            //     AND  s.nationkey = n.nationkey 
            // """
            // ,
            // s"""
            //   -- TPC-H Query 5
            //   SELECT n.name, SUM(l.extendedprice * (1 - l.discount)) AS revenue 
            //    FROM   region r, nation_run_$i n, supplier_run_$i s, customer_run_$i c, orders_run_$i o, lineitem_run_$i l 
            //   WHERE  r.name = 'ASIA'
            //     AND  n.regionkey = r.regionkey 
            //     AND  c.custkey = o.custkey
            //     AND  o.orderdate >= DATE('1994-01-01')
            //     AND  o.orderdate <  DATE('1995-01-01')
            //     AND  l.orderkey = o.orderkey 
            //     AND  l.suppkey = s.suppkey
            //     AND  c.nationkey = s.nationkey 
            //     AND  s.nationkey = n.nationkey 
            //   GROUP BY n.name
            // """
            // ,
            // s"""
            //   -- TPC-H Query 9
            //   SELECT nation, o_year, SUM(amount) AS sum_profit 
            //   FROM (
            //     SELECT n.name AS nation, 
            //            EXTRACT(year from o.orderdate) AS o_year,
            //            ((l.extendedprice * (1 - l.discount)) - (ps.supplycost * l.quantity))
            //               AS amount
            //     FROM  part p, 
            //           partsupp_run_$i ps, 
            //           supplier_run_$i s, 
            //           lineitem_run_$i l, 
            //           orders_run_$i o, 
            //           nation_run_$i n
            //     WHERE  (p.name LIKE '%green%')
            //       AND  ps.partkey = l.partkey
            //       AND  ps.suppkey = l.suppkey 
            //       AND  p.partkey = l.partkey
            //       AND  s.suppkey = l.suppkey
            //       AND  o.orderkey = l.orderkey 
            //       AND  s.nationkey = n.nationkey 
            //     ) AS profit 
            //   GROUP BY nation, o_year;
            // """
          )


        sequential

        
        // CREATE LENSES
        Fragments.foreach(
          relevantTables.toSeq
        ){ createMissingValueLens(_, s"_RUN_$i") }

        // // INDEXES
        /*if(useMaterialized){
          Fragments.foreach( relevantIndexes ) {
            case (baseTable, indices) => 
              val viewTable = s"${baseTable}_RUN_$i"
              Fragments.foreach(indices){ index =>
                s"Create Index $viewTable(${index.mkString(",")})" >> {
                  val indexName = viewTable+"_"+index.mkString("_")
                  println(s"CREATE INDEX $indexName")
                  db.backend.update(s"CREATE INDEX IF NOT EXISTS $indexName ON $viewTable(${index.mkString(",")})")
                  ok
                }
              }
          }
        } else { "No Need To Create Indexes" >> ok }*/

        // QUERIES
        if(runBestGuessQueries){
          Fragments.foreach( TPCHQueries.zipWithIndex )
          {
            queryLens(_)
          }
        } else { "Skipping Best Guess Tests" >> ok }

        if(runTupleBundleQueries){
          Fragments.foreach(TPCHQueries.zipWithIndex)
          {
            sampleFromLens(_)
          }
        } else { "Skipping Tuple Bundle Tests" >> ok }

        if(runSamplerQueries){
          Fragments.foreach(TPCHQueries.zipWithIndex)
          {
            expectedFromLens(_)
          }
        } else { "Skipping Sampler Tests" >> ok }


      }

    }
  }
}
