package mimir.lenses

import java.io._
import org.specs2.specification._

import mimir.algebra._
import mimir.util._
import mimir.ctables.{VGTerm}
import mimir.optimizer.{InlineVGTerms,InlineProjections}
import mimir.test.{SQLTestSpecification, PDBench, TestTimer}
import mimir.models._
import org.specs2.specification.core.Fragments

object RepairKeyTimingSpec
  extends SQLTestSpecification("RepairKeyTiming", Map("reset" -> "NO", "inline" -> "YES"))
  with BeforeAll
  with TestTimer
{

  sequential

  args(skipAll = !PDBench.isDownloaded)

  val skipBestGuessQueries = true
  val skipTupleBundleQueries = false

  def beforeAll =
  {
    // update("DELETE FROM MIMIR_MODEL_OWNERS")
    // update("DELETE FROM MIMIR_MODELS")
    // update("DELETE FROM MIMIR_VIEWS")
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

  "The Key Repair Lens Timing" should {

    sequential
    Fragments.foreach(1 to 1){ i =>
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
      Fragments.foreach(
        if(skipBestGuessQueries){ Seq() } else { Seq(
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
           """, 60.0 ),
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
           """, 60.0 ),
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
                and cnk.tid = ck.tid
                and nk2.tid = nn2.tid and nk1.tid = nn1.tid
           """, 60.0 )
      ).zipWithIndex}){
          queryKeyRepairLens(_)
        }

      Fragments.foreach(
        if(skipBestGuessQueries){ Seq() } else { Seq(
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
        ).zipWithIndex}){
          sampleFromKeyRepairLens(_)
        }


    }

  }

  def loadTable(tableFields:(String, String, Type, Double)) =
  {
    val (baseTable, columnName, columnType, timeout) = tableFields
    s"Load Table: $baseTable" >> {
      if(!db.tableExists(baseTable)){
        update(s"""
          CREATE TABLE $baseTable(
            VAR_ID int,
            WORLD_ID int,
            TID int,
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
      db.tableExists(baseTable) must beTrue
    }
  }

  def createKeyRepairLens(tableFields:(String, String, Type, Double), tableSuffix: String = "_cleaned") =  
  {
    val (baseTable, columnName, columnType, timeout) = tableFields
    val testTable = (baseTable+tableSuffix).toUpperCase

    s"Create Key Repair Lens for table: $testTable" >> {
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
 
  def createKeyRepairRowWiseLens(tableData: (String, (Seq[String], Seq[(String,String,Type,Double)])), tableSuffix: String = "_cleaned") =
  {
    val (baseTable, (tableKeys, tableFields)) = tableData
    val testTable = (baseTable+tableSuffix).toUpperCase

    s"Create Key Repair Lens for table: $testTable" >> {
      val sourceProjections: Seq[(String, Operator)] = 
        tableFields.map { case (columnTable, colName, _, _) =>
          val tidCol = s"TID_${colName.toUpperCase}"
          (
            tidCol,
            Project(Seq(
              ProjectArg(tidCol, Var("TID")),
              ProjectArg(colName.toUpperCase, Var(colName.toUpperCase))
            ), db.getTableOperator(columnTable))
          )
        }

      val joined: (String,Operator) = 
        sourceProjections.tail.fold(sourceProjections.head) { 
          case ((tidLeft: String, sourceLeft: Operator), (tidRight: String, sourceRight: Operator)) =>
          ( tidLeft,
            Select(
              Comparison(Cmp.Eq, Var(tidLeft), Var(tidRight)),
              Join(sourceLeft, sourceRight)
            )
          )
        }

      val justTheAttributes = 
        Project(
          tableFields.map(_._2).map { field => ProjectArg(field.toUpperCase, Var(field.toUpperCase)) },
          joined._2
        )

      db.lenses.create(
        "KEY_REPAIR", 
        testTable,
        justTheAttributes, 
        Seq(Var("TID"))
      )
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

  def queryKeyRepairLens(config : ((String, Double), Int)) = 
  {
    val ((queryString, timeout), idx) = config
    s"Query Key Repair Lens : ${queryString}" >> {
      val ((rows, backendTime), totalTime) = time {
        var x = 0
        val backendTime = query(queryString) { results =>
          time { results.foreach { row => (x = x + 1) } } 
        }
        (x, backendTime._2)
      }
      println(s"Time:${totalTime} seconds (${backendTime} seconds reading $rows results);  <- Query $idx: \n$queryString")
      rows must be_>(0)
      totalTime should be lessThan timeout+2 // Add 2 seconds for the optimizer (for now)
    }
  }

  def sampleFromKeyRepairLens(config : (String, Int)) =  
  {
    val (queryString, idx) = config
    s"Sample From Key Repair Lens : ${queryString}" >> {
      val ((rows, backendTime), totalTime) = time {
         var x = 0
         val backendTime = db.sampleQuery(queryString) { results =>
           time { results.foreach { row => (x = x + 1) } }
         }
         (x, backendTime._2)
      }
      println(s"Time:${totalTime} seconds (${backendTime} seconds reading $rows results);  <- Query $idx: \n$queryString")
      rows must be_>(0)
    }
  }

}
