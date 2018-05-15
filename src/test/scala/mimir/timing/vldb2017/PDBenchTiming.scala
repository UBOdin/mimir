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

object PDBenchTiming
  extends VLDB2017TimingTest("VLDB2017PDBench", Map("reset" -> "NO", "inline" -> "YES"))
  with BeforeAll
{

  sequential

  args(skipAll = !PDBench.isDownloaded)

  val fullReset = true
  val runBestGuessQueries = false
  val runTupleBundleQueries = false
  val runSamplerQueries = true
  val runPartitionQueries = false
  val useMaterialized = false

  val timeout = 10.minute

  def beforeAll =
  {
    if(fullReset){
      println("DELETING ALL MIMIR METADATA")
      update("DELETE FROM MIMIR_MODEL_OWNERS")
      update("DELETE FROM MIMIR_MODELS")
      update("DELETE FROM MIMIR_VIEWS")
    }
  }

  val relevantTables = Set(
    "customer",
    "orders",
    "lineitem",
    "nation",
    "supplier"
  )

  val relevantAttributes = Set(
    "cust_c_custkey",
    "cust_c_mktsegment",
    "cust_c_nationkey",
    "lineitem_l_discount",
    "lineitem_l_extendedprice",
    "lineitem_l_orderkey",
    "lineitem_l_quantity",
    "lineitem_l_shipdate",
    "lineitem_l_suppkey",
    "nation_n_name",
    "nation_n_nationkey",
    "orders_o_custkey",
    "orders_o_orderdate",
    "orders_o_orderkey",
    "orders_o_shippriority",
    "supp_s_nationkey",
    "supp_s_suppkey"
  )

  if(false){ "Skipping TPCH Inpute Test" >> ok } else {
    "PDBench" should {

      sequential
      Fragments.foreach(1 to 1){ i =>

        val PDBenchQueries = 
          Seq(
            s"""
                select ok.orderkey, od.orderdate, os.shippriority
                from cust_c_mktsegment_run_$i cs, cust_c_custkey_run_$i cck,
                     orders_o_orderkey_run_$i ok, orders_o_orderdate_run_$i od,
                     orders_o_shippriority_run_$i os, orders_o_custkey_run_$i ock,
                     lineitem_l_orderkey_run_$i lok, lineitem_l_shipdate_run_$i lsd
                where od.orderdate > DATE('1995-03-15')
                  and lsd.shipdate < DATE('1995-03-17')
                  and cs.mktsegment = 'BUILDING'
                  and lok.tid = lsd.tid
                  and cck.tid = cs.tid
                  and cck.custkey = ock.custkey
                  and ok.tid = ock.tid 
                  and ok.orderkey = lok.orderkey
                  and od.tid = ok.tid 
                  and os.tid = ok.tid
            """
            // ,
            // s"""
            //     select liep.extendedprice
            //     from lineitem_l_extendedprice_run_$i liep, lineitem_l_shipdate_run_$i lisd,
            //          lineitem_l_discount_run_$i lidi, lineitem_l_quantity_run_$i liq
            //     where lisd.shipdate between DATE('1994-01-01') and DATE('1996-01-01')
            //       and lidi.discount between 0.05 and 0.08
            //       and liq.quantity < 24
            //       and lisd.tid = lidi.tid
            //       and liq.tid = lidi.tid
            //       and liep.tid = liq.tid
            //  """
            //  ,
             // s"""
             //    select nn1.name, nn2.name
             //    from supp_s_suppkey_run_$i sk, supp_s_nationkey_run_$i snk,
             //         lineitem_l_orderkey_run_$i lok, lineitem_l_suppkey_run_$i lsk,
             //         orders_o_orderkey_run_$i ok, orders_o_custkey_run_$i ock,
             //         cust_c_custkey_run_$i ck, cust_c_nationkey_run_$i cnk,
             //         nation_n_name_run_$i nn1, nation_n_name_run_$i nn2,
             //         nation_n_nationkey_run_$i nk1, nation_n_nationkey_run_$i nk2
             //    where nn2.name='IRAQ' and nn1.name='GERMANY'
             //      and cnk.nationkey = nk2.nationkey 
             //      and snk.nationkey = nk1.nationkey
             //      and sk.suppkey = lsk.suppkey
             //      and ok.orderkey = lok.orderkey
             //      and ck.custkey = ock.custkey
             //      and lok.tid = lsk.tid
             //      and ock.tid = ok.tid
             //      and snk.tid = sk.tid
             //      and cnk.tid = ck.tid
             //      and nk2.tid = nn2.tid and nk1.tid = nn1.tid
             // """
          )


        sequential

        // LOAD DATA
        Fragments.foreach(
          relevantTables.toSeq.flatMap { PDBench.tables(_)._2 }
        ){ loadTable(_) }

        // CREATE COLUMNAR LENSES
        Fragments.foreach(
          PDBench.attributes.
            filter( x => relevantAttributes(x._1) )
        ){ createKeyRepairLens(_, s"_run_$i") }

        // CREATE ROW-WISE LENSES
        // Fragments.foreach(
        //   relevantTables.map { table => (table, PDBench.tables(table)) }.toSeq
        // ){ createKeyRepairRowWiseLens(_, s"_run_$i") }

        // QUERIES
        if(runBestGuessQueries){
          Fragments.foreach( PDBenchQueries.zipWithIndex ) { queryLens(_) }
        } else { "Skipping Best Guess Queries" >> ok }

        if(runTupleBundleQueries){
          Fragments.foreach( PDBenchQueries.zipWithIndex ) { sampleFromLens(_) }
        } else { "Skipping Tuple Bundle Queries" >> ok }

        if(runSamplerQueries){
          Fragments.foreach( PDBenchQueries.zipWithIndex ) { expectedFromLens(_) }
        } else { "Skipping Sampler Queries" >> ok }

        if(runPartitionQueries){
          Fragments.foreach( PDBenchQueries.zipWithIndex ) { partitionLens(_) }
        } else { "Skipping Partition Queries" >> ok }


      }
    }
  }
}
