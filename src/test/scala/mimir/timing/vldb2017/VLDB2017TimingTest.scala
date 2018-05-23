package mimir.timing.vldb2017

import java.util.Random
import java.io.File

import org.specs2.specification._
import org.specs2.specification.core.Fragments
import org.specs2.concurrent._
import scala.concurrent.duration._


import mimir.test.{SQLTestSpecification, PDBench, TestTimer}
import mimir.util._
import mimir.algebra._
import mimir.exec.mode._
import mimir.exec.uncertainty._

abstract class VLDB2017TimingTest(dbName: String, config: Map[String,String])
  extends SQLTestSpecification(dbName, config)
  with TestTimer
  with AroundTimeout
{
  sequential
  args.execute(threadsNb = 1)
  val timeout: Duration
  val useMaterialized: Boolean
  val random = new Random(42)
  val tupleBundle = new TupleBundle( (0 until 10).map { _ => random.nextLong })
  val sampler     = new SampleRows( (0 until 10).map { _ => random.nextLong })


  def loadTable(tableFields:(String, String, Type, Double), run:Int=1) =
  {
    println(s"VLDB2017TimingTest.loadTable(${tableFields})")
    val (baseTable, columnName, columnType, timeout) = tableFields
    val dbTableName = baseTable.toUpperCase() 
    s"Load Table: $baseTable" >> {
      if(!db.tableExists(dbTableName)){
        val schema = Seq(("VAR_ID", TInt()),
            ("WORLD_ID", TInt()),
            ("TID", TInt()),
            (columnName.toUpperCase(), columnType))
        LoadCSV.handleLoadTableRaw(db, 
              dbTableName, 
              Some(schema),  new File(s"test/pdbench/${baseTable}.tbl"),  
              Map("DELIMITER" -> "|","ignoreLeadingWhiteSpace"->"true",
                  "ignoreTrailingWhiteSpace"->"true", 
                  "mode" -> "PERMISSIVE", 
                  "header" -> "false") )
      }
      db.tableExists(dbTableName) must beTrue
    }
  }

  def createKeyRepairLens(tableFields:(String, String, Type, Double), tableSuffix: String) =  
  {
    val (baseTable, columnName, columnType, timeout) = tableFields
    val testTable = (baseTable+tableSuffix).toUpperCase
    s"Create Key Repair Lens for table: $testTable" >> {
      if(db.views.get(testTable).isEmpty){
        if(db.tableExists(testTable)){
          update(s"DROP TABLE $testTable")
        }
        if(db.tableExists("MIMIR_FASTPATH_"+testTable)){
          update(s"DROP TABLE MIMIR_FASTPATH_$testTable")
        }
        if(db.tableExists("MIMIR_KR_SOURCE_"+testTable)){
          update(s"DROP TABLE MIMIR_KR_SOURCE_${testTable}")
        }
        val timeForQuery = time {
        update(s"""
          CREATE LENS ${testTable}
            AS SELECT TID, ${columnName.toUpperCase()} FROM ${baseTable}
          WITH KEY_REPAIR(TID, ENABLE(FAST_PATH))
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
    s"Materialize Lens Source: $testTable" >> {
      if(!db.views("MIMIR_KR_SOURCE_"+testTable).isMaterialized){
        val timeForQuery = time {
          update(s"ALTER VIEW MIMIR_KR_SOURCE_${testTable} MATERIALIZE")
        }
        println(s"Materialize Time:${timeForQuery._2} seconds <- RepairKeyLens:MIMIR_KR_SOURCE_${testTable}")
      }
      db.views(testTable).isMaterialized must beTrue
    }
    s"Materialize Lens: $testTable" >> {
      if(useMaterialized){
        if(!db.views(testTable).isMaterialized){
          val timeForQuery = time {
            update(s"ALTER VIEW ${testTable} MATERIALIZE")
          }
          println(s"Materialize Time:${timeForQuery._2} seconds <- RepairKeyLens:${testTable}")
        }
        db.views(testTable).isMaterialized must beTrue
      } else {
        if(db.views(testTable).isMaterialized){
          update(s"ALTER VIEW ${testTable} DROP MATERIALIZE")
        }
        db.views(testTable).isMaterialized must beFalse
      }
    }
  }

  def createMissingValueLens(tableFields:(String, Seq[String]), tableSuffix: String = "_cleaned") =  
  {
    val (baseTable, nullables) = tableFields
    val testTable = (baseTable+tableSuffix).toUpperCase
    s"Create Missing Value Imputation Lens for table: $testTable" >> {
      // if(db.tableExists(testTable)){
      //   update(s"DROP LENS $testTable")
      // }
      if(db.views.get(testTable).isEmpty){
        if(db.tableExists(testTable)){
          update(s"DROP TABLE $testTable")
        }
        val timeForQuery = time {
          update(s"""
            CREATE LENS ${testTable}
              AS SELECT * FROM ${baseTable}
            WITH MISSING_VALUE(${nullables.map {"'"+_+"'"}.mkString(", ")})
          """);
        }
        println(s"Create Time:${timeForQuery._2} seconds <- RepairKeyLens:${testTable}")
      }
      db.tableExists(testTable) must beTrue
    }
    s"Materialize Lens: $testTable" >> {
      if(useMaterialized){
        if(!db.views(testTable).isMaterialized){
          val timeForQuery = time {
            update(s"ALTER VIEW ${testTable} MATERIALIZE")
          }
          println(s"Materialize Time:${timeForQuery._2} seconds <- RepairKeyLens:${testTable}")
        }
        db.views(testTable).isMaterialized must beTrue
      } else {
        if(db.views(testTable).isMaterialized){
          update(s"ALTER VIEW ${testTable} DROP MATERIALIZE")
        }
        db.views(testTable).isMaterialized must beFalse
      }
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
            ), db.table(columnTable))
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

  def queryLens(config : (String, Int)) = 
  {
    val (queryString, idx) = config
    s"Query Lens ($idx): ${queryString}" >> {
      implicit ee: ExecutionEnv => 
        upTo(timeout){
          try {
            println(s"GUESS QUERY: \n$queryString")
            val ((rows, backendTime), totalTime) = time {
              var x = 0
              val backendTime = query(queryString) { results =>
                time { results.foreach { row => (x = x + 1) } } 
              }
              (x, backendTime._2)
            }
            println(s"Time:${totalTime} seconds (${backendTime} seconds reading $rows results);  <- Query $idx: \n$queryString")
            rows must be_>(0)
          } catch {
            case e: Throwable => {
              e.printStackTrace
              ko
            }
          }
        }
    }
  }

  def sampleFromLens(config : (String, Int)) =  
  {
    val (queryString, idx) = config
    s"Sample From Lens ($idx): ${queryString}" >> {
      implicit ee: ExecutionEnv => 
        upTo(timeout){
          try {
            println(s"SAMPLE QUERY $idx:\n$queryString")
            val ((rows, backendTime), totalTime) = time {
               var x = 0
               val backendTime = db.query(queryString) { results =>
                 time { results.foreach { row => (x = x + 1) } }
               }
               (x, backendTime._2)
            }
            println(s"Time:${totalTime} seconds (${backendTime} seconds reading $rows results);  <- Query $idx: \n$queryString")
            rows must be_>(0)
          } catch {
            case e: Throwable => {
              e.printStackTrace
              ko
            }
          }
        }
    }
  }

  def expectedFromLens(config : (String, Int)) =  
  {
    val (queryString, idx) = config
    s"Expectation, StdDev, and Confidence From Lens ($idx): ${queryString}" >> {
      implicit ee: ExecutionEnv => 
        upTo(timeout){
          try {
            println(s"EXPECTATION QUERY $idx: $queryString")
            val ((rows, backendTime), totalTime) = time {
              db.query(queryString, sampler) { results =>

                var x = 0
  
                val backendTime = time {
                  results.foreach { row => (x = x + 1) }
                }

                (x, backendTime._2)
              }
            }

            println(s"Time:${totalTime} seconds (${backendTime} seconds reading $rows results);  <- Query $idx: \n$queryString")
            rows must be_>(0)
          } catch {
            case e: Throwable => {
              e.printStackTrace
              ko
            }
          }
        }
    }
  }

  def partitionLens(config : (String, Int)) =
  {
    val (queryString, idx) = config
    s"Partitioning Lens ($idx): ${queryString}" >> {
      implicit ee: ExecutionEnv => 
        upTo(timeout){
          try {
            println(s"PARTITION QUERY $idx: $queryString")
            val ((rows, backendTime), totalTime) = time {
              db.query(queryString, Partition) { results =>
                var x = 0
                val backendTime = time {
                  results.foreach { row => (x = x + 1) }
                }
                (x, backendTime._2)
              }
            }
            println(s"Time:${totalTime} seconds (${backendTime} seconds reading $rows results);  <- Query $idx: \n$queryString")
            rows must be_>(0)
          } catch {
            case e: Throwable => {
              e.printStackTrace
              ko
            }
          }
        }
    }
  }
  
  /*
SQLite Timing
   
PDB	
time printf "create table q1_sqlite_time as select ok.orderkey, od.orderdate, os.shippriority from cust_c_mktsegment_run_1 cs, cust_c_custkey_run_1 cck, orders_o_orderkey_run_1 ok, orders_o_orderdate_run_1 od, orders_o_shippriority_run_1 os, orders_o_custkey_run_1 ock, lineitem_l_orderkey_run_1 lok, lineitem_l_shipdate_run_1 lsd where od.orderdate > DATE('1995-03-15') and lsd.shipdate < DATE('1995-03-17') and cs.mktsegment = 'BUILDING' and lok.tid = lsd.tid and cck.tid = cs.tid and cck.custkey = ock.custkey and ok.tid = ock.tid and ok.orderkey = lok.orderkey and od.tid = ok.tid and os.tid = ok.tid;" | sqlite3 VLDB2017PDBench.db > /dev/null 	9.521
time printf "create table q2_sqlite_time as select liep.extendedprice from lineitem_l_extendedprice_run_1 liep, lineitem_l_shipdate_run_1 lisd, lineitem_l_discount_run_1 lidi, lineitem_l_quantity_run_1 liq where lisd.shipdate between DATE('1994-01-01') and DATE('1996-01-01') and lidi.discount between 0.05 and 0.08 and liq.quantity < 24 and lisd.tid = lidi.tid and liq.tid = lidi.tid and liep.tid = liq.tid;" | sqlite3 VLDB2017PDBench.db > /dev/null 	7.59
time printf "create table q3_sqlite_time as select nn1.name, nn2.name from supp_s_suppkey_run_1 sk, supp_s_nationkey_run_1 snk, lineitem_l_orderkey_run_1 lok, lineitem_l_suppkey_run_1 lsk, orders_o_orderkey_run_1 ok, orders_o_custkey_run_1 ock, cust_c_custkey_run_1 ck, cust_c_nationkey_run_1 cnk, nation_n_name_run_1 nn1, nation_n_name_run_1 nn2, nation_n_nationkey_run_1 nk1, nation_n_nationkey_run_1 nk2 where nn2.name='IRAQ' and nn1.name='GERMANY' and cnk.nationkey = nk2.nationkey and snk.nationkey = nk1.nationkey and sk.suppkey = lsk.suppkey and ok.orderkey = lok.orderkey and ck.custkey = ock.custkey and lok.tid = lsk.tid and ock.tid = ok.tid and snk.tid = sk.tid and cnk.tid = ck.tid and nk2.tid = nn2.tid and nk1.tid = nn1.tid;" | sqlite3 VLDB2017PDBench.db > /dev/null 	31.22
	
TPCH	
time printf "create table tpch_q1_sqlite_time as select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice *(1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= DATE('1997-09-01') group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;" | sqlite3 source/mimir/MCDBTiming.db > /dev/null  	19.561
time printf "create table tpch_q3_sqlite_time as  select l_orderkey, sum(l_extendedprice *(1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < DATE('1995-03-15') and l_shipdate > DATE('1995-03-15') group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate; " | sqlite3 source/mimir/MCDBTiming.db > /dev/null  	22.835
time printf "create table tpch_q5_sqlite_time as  select n_name, sum(l_extendedprice *(1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= DATE('1994-01-01') and o_orderdate < DATE(‘1995-01-01') group by n_name order by revenue desc; " | sqlite3 source/mimir/MCDBTiming.db > /dev/null  	33.308
time printf "create table tpch_q9_sqlite_time as  select nation, o_year, sum(amount) as sum_profit from (select n_name as nation, strftime('%Y', o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from part, supplier, lineitem, partsupp, orders, nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like 'green') as profit group by nation, o_year order by nation, o_year desc;" | sqlite3 source/mimir/MCDBTiming.db > /dev/null  	51.125
   
   */

}