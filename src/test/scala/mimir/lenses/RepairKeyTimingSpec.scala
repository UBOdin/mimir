package mimir.lenses

import java.io._
import org.specs2.specification._

import mimir.algebra._
import mimir.util._
import mimir.ctables.{VGTerm}
import mimir.optimizer.{ResolveViews,InlineVGTerms,InlineProjections}
import mimir.test._
import mimir.models._
import org.specs2.specification.core.Fragments

object RepairKeyTimingSpec
  extends SQLTestSpecification("RepairKeyTiming", Map("reset" -> "NO", "inline" -> "YES"))
  with BeforeAll
{

  sequential

  def beforeAll =
  {
    ExperimentalOptions.enable("NO-BG-CACHE")
    // update("DELETE FROM MIMIR_MODEL_OWNERS")
    // update("DELETE FROM MIMIR_MODELS")
    // update("DELETE FROM MIMIR_VIEWS")
  }

  val relevantTables = Set(
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

  "The Key Repair Lens Timing" should {

    sequential
    Fragments.foreach(1 to 1){ i =>
      sequential
      Fragments.foreach(Seq(
        (s"cust_c_acctbal_run_$i",           "cust_c_acctbal",           "acctbal",       TFloat(),  5000000),
        (s"cust_c_address_run_$i",           "cust_c_address",           "address",       TString(), 5000000),
        (s"cust_c_comment_run_$i",           "cust_c_comment",           "comment",       TString(), 5000000),
        (s"cust_c_custkey_run_$i",           "cust_c_custkey",           "custkey",       TInt(),    5000000),
        (s"cust_c_mktsegment_run_$i",        "cust_c_mktsegment",        "mktsegment",    TString(), 5000000),
        (s"cust_c_name_run_$i",              "cust_c_name",              "name",          TString(), 5000000),
        (s"cust_c_nationkey_run_$i",         "cust_c_nationkey",         "nationkey",     TInt(),    5000000),
        (s"cust_c_phone_run_$i",             "cust_c_phone",             "phone",         TString(), 5000000),

        (s"lineitem_l_comment_run_$i",       "lineitem_l_comment",       "comment",       TString(), 5000000),
        (s"lineitem_l_commitdate_run_$i",    "lineitem_l_commitdate",    "commitdate",    TDate(),   5000000),
        (s"lineitem_l_discount_run_$i",      "lineitem_l_discount",      "discount",      TFloat(),  5000000),
        (s"lineitem_l_extendedprice_run_$i", "lineitem_l_extendedprice", "extendedprice", TFloat(),  5000000),
        (s"lineitem_l_linenumber_run_$i",    "lineitem_l_linenumber",    "linenumber",    TInt(),    5000000),
        (s"lineitem_l_linestatus_run_$i",    "lineitem_l_linestatus",    "linestatus",    TString(), 5000000),
        (s"lineitem_l_orderkey_run_$i",      "lineitem_l_orderkey",      "orderkey",      TInt(),    5000000),
        (s"lineitem_l_partkey_run_$i",       "lineitem_l_partkey",       "partkey",       TInt(),    5000000),
        (s"lineitem_l_quantity_run_$i",      "lineitem_l_quantity",      "quantity",      TInt(),    5000000),
        (s"lineitem_l_receiptdate_run_$i",   "lineitem_l_receiptdate",   "receiptdate",   TDate(),   5000000),
        (s"lineitem_l_returnflag_run_$i",    "lineitem_l_returnflag",    "returnflag",    TString(), 5000000),
        (s"lineitem_l_shipdate_run_$i",      "lineitem_l_shipdate",      "shipdate",      TDate(),   5000000),
        (s"lineitem_l_shipinstruct_run_$i",  "lineitem_l_shipinstruct",  "shipinstruct",  TString(), 5000000),
        (s"lineitem_l_shipmode_run_$i",      "lineitem_l_shipmode",      "shipmode",      TString(), 5000000),
        (s"lineitem_l_suppkey_run_$i",       "lineitem_l_suppkey",       "suppkey",       TInt(),    5000000),
        (s"lineitem_l_tax_run_$i",           "lineitem_l_tax",           "tax",           TFloat(),  5000000),

        (s"nation_n_comment_run_$i",         "nation_n_comment",         "comment",       TString(), 5000000),
        (s"nation_n_name_run_$i",            "nation_n_name",            "name",          TString(), 5000000),
        (s"nation_n_nationkey_run_$i",       "nation_n_nationkey",       "nationkey",     TInt(),    5000000),
        (s"nation_n_regionkey_run_$i",       "nation_n_regionkey",       "regionkey",     TInt(),    5000000),

        (s"orders_o_clerk_run_$i",           "orders_o_clerk",           "clerk",         TString(), 5000000),
        (s"orders_o_comment_run_$i",         "orders_o_comment",         "comment",       TString(), 5000000),
        (s"orders_o_custkey_run_$i",         "orders_o_custkey",         "custkey",       TInt(),    5000000),
        (s"orders_o_orderdate_run_$i",       "orders_o_orderdate",       "orderdate",     TDate(),   5000000),
        (s"orders_o_orderkey_run_$i",        "orders_o_orderkey",        "orderkey",      TInt(),    5000000),
        (s"orders_o_orderpriority_run_$i",   "orders_o_orderpriority",   "orderpriority", TString(), 5000000),
        (s"orders_o_orderstatus_run_$i",     "orders_o_orderstatus",     "orderstatus",   TString(), 5000000),
        (s"orders_o_shippriority_run_$i",    "orders_o_shippriority",    "shippriority",  TString(), 5000000),
        (s"orders_o_totalprice_run_$i",      "orders_o_totalprice",      "totalprice",    TFloat(), 5000000),

        (s"part_p_brand_run_$i",             "part_p_brand",             "brand",         TString(), 5000000),
        (s"part_p_comment_run_$i",           "part_p_comment",           "comment",       TString(), 5000000),
        (s"part_p_container_run_$i",         "part_p_container",         "container",     TString(), 5000000),
        (s"part_p_mfgr_run_$i",              "part_p_mfgr",              "mfgr",          TString(), 5000000),
        (s"part_p_name_run_$i",              "part_p_name",              "name",          TString(), 5000000),
        (s"part_p_partkey_run_$i",           "part_p_partkey",           "partkey",       TInt(),    5000000),
        (s"part_p_retailprice_run_$i",       "part_p_retailprice",       "retailprice",   TFloat(),  5000000),
        (s"part_p_size_run_$i",              "part_p_size",              "size",          TString(), 5000000),
        (s"part_p_type_run_$i",              "part_p_type",              "type",          TString(), 5000000),

        (s"psupp_ps_availqty_run_$i",        "psupp_ps_availqty",        "availqty",      TInt(),    5000000),
        (s"psupp_ps_comment_run_$i",         "psupp_ps_comment",         "comment",       TString(), 5000000),
        (s"psupp_ps_partkey_run_$i",         "psupp_ps_partkey",         "partkey",       TInt(),    5000000),
        (s"psupp_ps_suppkey_run_$i",         "psupp_ps_suppkey",         "suppkey",       TInt(),    5000000),
        (s"psupp_ps_supplycost_run_$i",      "psupp_ps_supplycost",      "supplycost",    TFloat(),  5000000),

        (s"region_r_comment_run_$i",         "region_r_comment",         "comment",       TString(), 5000000),
        (s"region_r_name_run_$i",            "region_r_name",            "name",          TString(), 5000000),
        (s"region_r_regionkey_run_$i",       "region_r_regionkey",       "regionkey",     TInt(),    5000000),

        (s"supp_s_acctbal_run_$i",           "supp_s_acctbal",           "acctbal",       TFloat(),  5000000),
        (s"supp_s_address_run_$i",           "supp_s_address",           "address",       TString(), 5000000),
        (s"supp_s_comment_run_$i",           "supp_s_comment",           "comment",       TString(), 5000000),
        (s"supp_s_name_run_$i",              "supp_s_name",              "name",          TString(), 5000000),
        (s"supp_s_nationkey_run_$i",         "supp_s_nationkey",         "nationkey",     TInt(),    5000000),
        (s"supp_s_phone_run_$i",             "supp_s_phone",             "phone",         TString(), 5000000),
        (s"supp_s_suppkey_run_$i",           "supp_s_suppkey",           "suppkey",       TInt(),    5000000)
      ).filter( x => relevantTables(x._2) )){
        tak =>  {
          {createKeyRepairLens(tak)}
        }
      }
      Fragments.foreach(Seq(
        ("""
            select ok.orderkey, od.orderdate, os.shippriority 
            from cust_c_mktsegment cs, cust_c_custkey cck, 
                 orders_o_orderkey ok, orders_o_orderdate od, 
                   orders_o_shippriority os, orders_o_custkey ock, 
                 lineitem_l_orderkey lok, lineitem_l_shipdate lsd 
            where cs.mktsegment = 'BUILDING' 
              and cs.tid = cck.tid 
              and cck.custkey = ock.custkey 
              and ok.orderkey = lok.orderkey 
              and od.orderdate > DATE('1995-03-15')
              and ock.tid = ok.tid and od.tid = ok.tid and os.tid = ok.tid
              and lsd.shipdate < DATE('1995-03-17')
              and lsd.tid = lok.tid
        """, 1976525000l ),
        ("""
            select liep.extendedprice 
            from lineitem_l_extendedprice liep, lineitem_l_shipdate lisd, 
                 lineitem_l_discount lidi, lineitem_l_quantity liq 
            where lisd.shipdate between DATE('1994-01-01') and DATE('1996-01-01')
              and lidi.tid = lisd.tid 
              and lidi.discount between 0.05 and 0.08
              and liq.tid = lidi.tid 
              and liq.quantity < 24 
              and liep.tid = liq.tid
        """, 14182710000l ),
        ("""
            select nn1.name, nn2.name 
            from supp_s_suppkey sk, supp_s_nationkey snk, 
                 lineitem_l_orderkey lok, lineitem_l_suppkey lsk, 
                 orders_o_orderkey ok, orders_o_custkey ock, 
                 cust_c_custkey ck, cust_c_nationkey cnk, 
                 nation_n_name nn1, nation_n_name nn2, 
                 nation_n_nationkey nk1, nation_n_nationkey nk2 
            where nn2.name='IRAQ' and nn1.name='GERMANY'
              and cnk.nationkey = nk2.nationkey and snk.nationkey = nk1.nationkey
              and sk.suppkey = lsk.suppkey 
              and ok.orderkey = lok.orderkey 
              and ck.custkey = ock.custkey 
              and lok.tid = lsk.tid 
              and ock.tid = ok.tid 
              and snk.tid = sk.tid 
              and nk2.tid = nn2.tid and nk1.tid = nn1.tid 
        """, 53196000l )
    )){
      qat =>  {
          {queryKeyRepairLens(qat)}
        }
      }
    }

  }

  def createKeyRepairLens(tableAndKeyColumn : (String, String, String, Type, Int)) =  s"Create Key Repair Lens for table: ${tableAndKeyColumn._2}" >> {
    if(!db.tableExists(tableAndKeyColumn._2)){
      update(s"""
        CREATE TABLE ${tableAndKeyColumn._2}(
          TID int,
          WORLD_ID int,
          VAR_ID int,
          ${tableAndKeyColumn._3} ${tableAndKeyColumn._4},
          PRIMARY KEY (TID, WORLD_ID, VAR_ID)
        )
      """)
      LoadCSV.handleLoadTable(db, 
        tableAndKeyColumn._2, 
        new File(s"test/maybms/${tableAndKeyColumn._2}.tbl"), 
        Map(
          "HEADER" -> "NO",
          "DELIMITER" -> "|"
        )
      )
    }
    if(!query(s"SELECT NAME FROM MIMIR_VIEWS WHERE NAME = '${tableAndKeyColumn._1}'").allRows.isEmpty){
      val timeForQuery = time {
      update(s"""
        CREATE LENS ${tableAndKeyColumn._1}
          AS SELECT TID, ${tableAndKeyColumn._3} FROM ${tableAndKeyColumn._2}
        WITH KEY_REPAIR(TID, ENABLE(FAST_PATH))
      """);
      }
      println(s"Time:${timeForQuery._2} nanoseconds <- RepairKeyLens:${tableAndKeyColumn._1}")
      timeForQuery._2 should be lessThan tableAndKeyColumn._5
    } else {
      ok
    }
  }

 def queryKeyRepairLens(queryAndTime : (String, Long)) =  s"Query Key Repair Lens : ${queryAndTime._1}" >> {
      val timeForQuery = time {
        query(queryAndTime._1)
     }
     println(s"Time:${timeForQuery._2} nanoseconds <- Query:${queryAndTime._1} ")
     timeForQuery._2 should be lessThan queryAndTime._2
  }

  def time[F](anonFunc: => F): (F, Long) = {
      val tStart = System.nanoTime()
      val anonFuncRet = anonFunc
      val tEnd = System.nanoTime()
      (anonFuncRet, tEnd-tStart)
    }

}
