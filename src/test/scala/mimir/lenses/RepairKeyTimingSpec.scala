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
    update("DELETE FROM MIMIR_MODEL_OWNERS")
    update("DELETE FROM MIMIR_MODELS")
    update("DELETE FROM MIMIR_VIEWS")
  }

  "The Key Repair Lens Timing" should {

    sequential
    Fragments.foreach(1 to 1){ i =>
      sequential
      Fragments.foreach(Seq(


        (s"cust_c_acctbal_run_$i",
        	"cust_c_acctbal",
        	"tid",
        	5000000),
        (s"cust_c_address_run_$i",
        	"cust_c_address",
        	"tid",
        	5000000),
        (s"cust_c_comment_run_$i",
        	"cust_c_comment",
        	"tid",
        	5000000),
        (s"cust_c_custkey_run_$i",
        	"cust_c_custkey",
        	"tid",
        	5000000),
        (s"cust_c_mktsegment_run_$i",
        	"cust_c_mktsegment",
        	"tid",
        	5000000),
        (s"cust_c_name_run_$i",
        	"cust_c_name",
        	"tid",
        	5000000),
        (s"cust_c_nationkey_run_$i",
        	"cust_c_nationkey",
        	"tid",
        	5000000),
        (s"cust_c_phone_run_$i",
        	"cust_c_phone",
        	"tid",
        	5000000),


        (s"lineitem_l_comment_run_$i",
        	"lineitem_l_comment",
        	"tid",
        	5000000),
        (s"lineitem_l_commitdate_run_$i",
        	"lineitem_l_commitdate",
        	"tid",
        	5000000),
        (s"lineitem_l_discount_run_$i",
        	"lineitem_l_discount",
        	"tid",
        	5000000),
        (s"lineitem_l_extendedprice_run_$i",
        	"lineitem_l_extendedprice",
        	"tid",
        	5000000),
        (s"lineitem_l_linenumber_run_$i",
        	"lineitem_l_linenumber",
        	"tid",
        	5000000),
        (s"lineitem_l_linestatus_run_$i",
        	"lineitem_l_linestatus",
        	"tid",
        	5000000),
        (s"lineitem_l_orderkey_run_$i",
        	"lineitem_l_orderkey",
        	"tid",
        	5000000),
        (s"lineitem_l_partkey_run_$i",
        	"lineitem_l_partkey",
        	"tid",
        	5000000),
        (s"lineitem_l_quantity_run_$i",
        	"lineitem_l_quantity",
        	"tid",
        	5000000),
        (s"lineitem_l_receiptdate_run_$i",
        	"lineitem_l_receiptdate",
        	"tid",
        	5000000),
        (s"lineitem_l_returnflag_run_$i",
        	"lineitem_l_returnflag",
        	"tid",
        	5000000),
        (s"lineitem_l_shipdate_run_$i",
        	"lineitem_l_shipdate",
        	"tid",
        	5000000),
        (s"lineitem_l_shipinstruct_run_$i",
        	"lineitem_l_shipinstruct",
        	"tid",
        	5000000),
        (s"lineitem_l_shipmode_run_$i",
        	"lineitem_l_shipmode",
        	"tid",
        	5000000),
        (s"lineitem_l_suppkey_run_$i",
        	"lineitem_l_suppkey",
        	"tid",
        	5000000),
        (s"lineitem_l_tax_run_$i",
        	"lineitem_l_tax",
        	"tid",
        	5000000),


        (s"nation_n_comment_run_$i",
        	"nation_n_comment",
        	"tid",
        	5000000),
        (s"nation_n_name_run_$i",
        	"nation_n_name",
        	"tid",
        	5000000),
        (s"nation_n_nationkey_run_$i",
        	"nation_n_nationkey",
        	"tid",
        	5000000),
        (s"nation_n_regionkey_run_$i",
        	"nation_n_regionkey",
        	"tid",
        	5000000),


        (s"orders_o_clerk_run_$i",
        	"orders_o_clerk",
        	"tid",
        	5000000),
        (s"orders_o_comment_run_$i",
        	"orders_o_comment",
        	"tid",
        	5000000),
        (s"orders_o_custkey_run_$i",
        	"orders_o_custkey",
        	"tid",
        	5000000),
        (s"orders_o_orderdate_run_$i",
        	"orders_o_orderdate",
        	"tid",
        	5000000),
        (s"orders_o_orderkey_run_$i",
        	"orders_o_orderkey",
        	"tid",
        	5000000),
        (s"orders_o_orderpriority_run_$i",
        	"orders_o_orderpriority",
        	"tid",
        	5000000),
        (s"orders_o_orderstatus_run_$i",
        	"orders_o_orderstatus",
        	"tid",
        	5000000),
        (s"orders_o_shippriority_run_$i",
        	"orders_o_shippriority",
        	"tid",
        	5000000),
        (s"orders_o_totalprice_run_$i",
        	"orders_o_totalprice",
        	"tid",
        	5000000),


        (s"part_p_brand_run_$i",
        	"part_p_brand",
        	"tid",
        	5000000),
        (s"part_p_comment_run_$i",
        	"part_p_comment",
        	"tid",
        	5000000),
        (s"part_p_container_run_$i",
        	"part_p_container",
        	"tid",
        	5000000),
        (s"part_p_mfgr_run_$i",
        	"part_p_mfgr",
        	"tid",
        	5000000),
        (s"part_p_name_run_$i",
        	"part_p_name",
        	"tid",
        	5000000),
        (s"part_p_partkey_run_$i",
        	"part_p_partkey",
        	"tid",
        	5000000),
        (s"part_p_retailprice_run_$i",
        	"part_p_retailprice",
        	"tid",
        	5000000),
        (s"part_p_size_run_$i",
        	"part_p_size",
        	"tid",
        	5000000),
        (s"part_p_type_run_$i",
        	"part_p_type",
        	"tid",
        	5000000),


        (s"psupp_ps_availqty_run_$i",
        	"psupp_ps_availqty",
        	"tid",
        	5000000),
        (s"psupp_ps_comment_run_$i",
        	"psupp_ps_comment",
        	"tid",
        	5000000),
        (s"psupp_ps_partkey_run_$i",
        	"psupp_ps_partkey",
        	"tid",
        	5000000),
        (s"psupp_ps_suppkey_run_$i",
        	"psupp_ps_suppkey",
        	"tid",
        	5000000),
        (s"psupp_ps_supplycost_run_$i",
        	"psupp_ps_supplycost",
        	"tid",
        	5000000),


        (s"region_r_comment_run_$i",
        	"region_r_comment",
        	"tid",
        	5000000),
        (s"region_r_name_run_$i",
        	"region_r_name",
        	"tid",
        	5000000),
        (s"region_r_regionkey_run_$i",
        	"region_r_regionkey",
        	"tid",
        	5000000),


        (s"supp_s_acctbal_run_$i",
        	"supp_s_acctbal",
        	"tid",
        	5000000),
        (s"supp_s_address_run_$i",
        	"supp_s_address",
        	"tid",
        	5000000),
        (s"supp_s_comment_run_$i",
        	"supp_s_comment",
        	"tid",
        	5000000),
        (s"supp_s_name_run_$i",
        	"supp_s_name",
        	"tid",
        	5000000),
        (s"supp_s_nationkey_run_$i",
        	"supp_s_nationkey",
        	"tid",
        	5000000),
        (s"supp_s_phone_run_$i",
        	"supp_s_phone",
        	"tid",
        	5000000),
        (s"supp_s_suppkey_run_$i",
        	"supp_s_suppkey",
        	"tid",
        	5000000)


        )){
        tak =>  {
          {createKeyRepairLens(tak)}
        }
      }
      Fragments.foreach(Seq(
        ("""
            select ok.orderkey, od.orderdate, os.shippriority 
            from cust_c_mktsegment cs, cust_c_custkey cck, 
                 orders_o_orderkey ok, orders_o_orderdate od, orders_o_shippriority os, orders_o_custkey ock, 
                 lineitem_l_orderkey lok, lineitem_l_shipdate lsd 
            where cs.mktsegment = 'BUILDING' 
              and cs.tid = cck.tid 
              and cck.custkey = ock.custkey 
              and ok.orderkey = lok.orderkey 
              and od.orderdate > ’1995-03-15’ 
              and ock.tid = ok.tid and od.tid = ok.tid and os.tid = ok.tid
              and lsd.shipdate < ’1995-03-17’ 
              and lsd.tid = lok.tid
        """, 5000000 ),
        ("""
            select liep.extendedprice 
            from lineitem_l_extendedprice liep, lineitem_l_shipdate lisd, 
                 lineitem_l_discount lidi, lineitem_l_quantity liq 
            where lisd.shipdate between ’1994-01-01’ and ’1996-01-01’
              and lidi.tid = lisd.tid 
              and lidi.discount between ’0.05’ and ’0.08’ 
              and liq.tid = lidi.tid 
              and liq.quantity < 24 
              and liep.tid = liq.tid
        """, 5000000 ),
        ("select nn1.name, nn2.name from supp_s_suppkey sk, supp_s_nationkey snk, lineitem_l_orderkey lok, lineitem_l_suppkey lsk, orders_o_orderkey ok, orders_o_custkey ock, cust_c_custkey ck, cust_c_nationkey cnk, nation_n_name nn1, nation_n_name nn2, nation_n_nationkey nk1, nation_n_nationkey nk2 where nn2.nation=’IRAQ’ and nn1.nation=’GERMANY’ and nk2.tid = nn2.tid and cnk.nationkey = nk2.nationkey and sk.suppkey = lsk.suppkey and lok.tid = lsk.tid and ok.orderkey = lok.orderkey and ock.tid = ok.tid and ck.custkey = ock.custkey and snk.tid = sk.tid and snk.nationkey = nk1.nationkey",
            5000000)
        )){
        qat =>  {
          {queryKeyRepairLens(qat)}
        }
      }
    }

  }

  def createKeyRepairLens(tableAndKeyColumn : (String, String, String, Int)) =  s"Create Key Repair Lens for table: ${tableAndKeyColumn._2} key: ${tableAndKeyColumn._3}" >> {
        val timeForQuery = time {
        update(s"""
          CREATE LENS ${tableAndKeyColumn._1}
            AS SELECT * FROM ${tableAndKeyColumn._2}
          WITH KEY_REPAIR(${tableAndKeyColumn._3})
        """);
        }
        println(s"Time:${timeForQuery._2} nanoseconds <- RepairKeyLens:${tableAndKeyColumn._1}")
        timeForQuery._2 should be lessThan tableAndKeyColumn._4
    }

 def queryKeyRepairLens(queryAndTime : (String, Int)) =  s"Query Key Repair Lens : ${queryAndTime._1}" >> {
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
