package mimir.lenses

import java.io._
import org.specs2.specification._

import mimir.algebra._
import mimir.util._
import mimir.ctables.{VGTerm}
import mimir.optimizer.{InlineVGTerms,InlineProjections}
import mimir.test.{SQLTestSpecification, PDBench}
import mimir.models._
import org.specs2.specification.core.Fragments

object RepairKeyTimingSpec
  extends SQLTestSpecification("RepairKeyTiming", Map("reset" -> "NO", "inline" -> "YES"))
  with BeforeAll
{

  sequential

  args(skipAll = !PDBench.isDownloaded)

  def beforeAll =
  {
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
      Fragments.foreach(
        PDBench.attributes.
          filter( x => relevantTables(x._1) )
      ){ createKeyRepairLens(_, s"_run_$i") }
      Fragments.foreach(Seq(
        (s"""
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
         """, 1.976525000 ),
        (s"""
            select liep.extendedprice
            from lineitem_l_extendedprice_run_$i liep, lineitem_l_shipdate_run_$i lisd,
                 lineitem_l_discount_run_$i lidi, lineitem_l_quantity_run_$i liq
            where lisd.shipdate between DATE('1994-01-01') and DATE('1996-01-01')
              and lidi.discount between 0.05 and 0.08
              and liq.quantity < 24
              and lisd.tid = lidi.tid
              and liq.tid = lidi.tid
              and liep.tid = liq.tid
         """, 14.182710000 ),
        (s"""
            select nn1.name, nn2.name
            from supp_s_suppkey_run_$i sk, supp_s_nationkey_run_$i snk,
                 lineitem_l_orderkey_run_$i lok, lineitem_l_suppkey_run_$i lsk,
                 orders_o_orderkey_run_$i ok, orders_o_custkey_run_$i ock,
                 cust_c_custkey_run_$i ck, cust_c_nationkey_run_$i cnk,
                 nation_n_name_run_$i nn1, nation_n_name_run_$i nn2,
                 nation_n_nationkey_run_$i nk1, nation_n_nationkey_run_$i nk2
            where nn2.name='IRAQ' and nn1.name='GERMANY'
              and cnk.nationkey = nk2.nationkey 
              and snk.nationkey = nk1.nationkey
              and sk.suppkey = lsk.suppkey
              and ok.orderkey = lok.orderkey
              and ck.custkey = ock.custkey
              and lok.tid = lsk.tid
              and ock.tid = ok.tid
              and snk.tid = sk.tid
              and nk2.tid = nn2.tid and nk1.tid = nn1.tid
         """, .053196000 )
    )){
      qat =>  {
          {queryKeyRepairLens(qat)}
        }
      }
    }

  }

  def createKeyRepairLens(tableFields:(String, String, Type, Double), tableSuffix: String = "_cleaned") =  
  {
    val (baseTable, columnName, columnType, timeout) = tableFields
    val testTable = (baseTable+tableSuffix).toUpperCase

    s"Create Key Repair Lens for table: $testTable" >> {
      if(!db.tableExists(baseTable)){
        update(s"""
          CREATE TABLE $baseTable(
            TID int,
            WORLD_ID int,
            VAR_ID int,
            $columnName $columnType,
            PRIMARY KEY (TID, WORLD_ID, VAR_ID)
          )
        """)
        LoadCSV.handleLoadTable(db, 
          baseTable, 
          new File(s"test/pdbench/${baseTable}.tbl"), 
          Map(
            "HEADER" -> "NO",
            "DELIMITER" -> "|"
          )
        )
      }
      if(!db.tableExists(testTable)){
        val fastPathCacheTable = "MIMIR_FASTPATH_"+testTable
        if(db.tableExists("MIMIR_FASTPATH_"+testTable)){
          update(s"DROP TABLE $fastPathCacheTable")
        }
        val timeForQuery = time {
        update(s"""
          CREATE LENS ${testTable}
            AS SELECT TID, ${columnName} FROM ${baseTable}
          WITH KEY_REPAIR(TID)
        """);
          //val materializeQuery = s"SELECT * FROM ${tableAndKeyColumn._1}_UNMAT"
          //val oper = db.sql.convert(db.parse(materializeQuery).head.asInstanceOf[Select])
          //db.selectInto(tableAndKeyColumn._1, oper)
        }
        println(s"Create Time:${timeForQuery._2} seconds <- RepairKeyLens:${testTable}")
        timeForQuery._2 should be lessThan timeout
      }
      db.tableExists(testTable) must beTrue
    }
    s"Materialize Lens: $testTable" >> {
      if(!db.views(testTable).isMaterialized){
        val timeForQuery = time {
          update(s"ALTER VIEW ${testTable} MATERIALIZE")
        }
        println(s"Materialize Time:${timeForQuery._2} seconds <- RepairKeyLens:${testTable}")
      }
      db.views(testTable).isMaterialized must beTrue
    }
  }

 def queryKeyRepairLens(queryAndTime : (String, Double)) =  s"Query Key Repair Lens : ${queryAndTime._1}" >> {
      val timeForQuery = time {
        var x = 0
        val r = query(queryAndTime._1) { _.foreach { row => x += 1 } }
        println(s"$x rows in the result")
     }
     println(s"Time:${timeForQuery._2} seconds <- Query:${queryAndTime._1} ")
     timeForQuery._2 should be lessThan queryAndTime._2
  }

  def time[F](anonFunc: => F): (F, Double) = {
      val tStart = System.nanoTime()
      val anonFuncRet = anonFunc
      val tEnd = System.nanoTime()
      (anonFuncRet, (tEnd-tStart).toDouble/1000.0/1000.0/1000.0)
    }

}
