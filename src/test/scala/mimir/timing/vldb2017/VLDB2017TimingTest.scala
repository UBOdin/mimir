package mimir.timing.vldb2017

import org.specs2.specification._
import org.specs2.specification.core.Fragments
import org.specs2.concurrent._
import scala.concurrent.duration._

import java.io.File

import mimir.test.{SQLTestSpecification, PDBench, TestTimer}
import mimir.util._
import mimir.algebra._
import mimir.exec.uncertainty._

abstract class VLDB2017TimingTest(dbName: String, config: Map[String,String])
  extends SQLTestSpecification(dbName, config)
  with TestTimer
  with AroundTimeout
{

  val timeout: Duration


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
      if(db.views.get(testTable).isEmpty){
        if(db.tableExists(testTable)){
          update(s"DROP TABLE $testTable")
        }
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
      if(!db.views(testTable).isMaterialized){
        val timeForQuery = time {
          update(s"ALTER VIEW ${testTable} MATERIALIZE")
        }
        println(s"Materialize Time:${timeForQuery._2} seconds <- RepairKeyLens:${testTable}")
      }
      db.views(testTable).isMaterialized must beTrue
    }
  }

  def createMissingValueLens(tableFields:(String, Seq[String]), tableSuffix: String = "_cleaned") =  
  {
    val (baseTable, nullables) = tableFields
    val testTable = (baseTable+tableSuffix).toUpperCase

    s"Create Missing Value Imputation Lens for table: $testTable" >> {
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

  def queryLens(config : ((String, Double), Int)) = 
  {
    val ((queryString, timeout), idx) = config
    s"Query Lens ($idx): ${queryString}" >> {
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

  def sampleFromLens(config : (String, Int)) =  
  {
    val (queryString, idx) = config
    s"Sample From Lens ($idx): ${queryString}" >> {
      implicit ee: ExecutionEnv => 
        upTo(timeout){
          println(s"SAMPLE QUERY $idx:\n$queryString")
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

  def expectedFromLens(config : (String, Int)) =  
  {
    val (queryString, idx) = config
    s"Expectation, StdDev, and Confidence From Lens ($idx): ${queryString}" >> {
      implicit ee: ExecutionEnv => 
        upTo(timeout){
          println(s"EXPECTATION QUERY $idx: $queryString")
          val ((rows, backendTime), totalTime) = time {
            var x = 0
            val queryStmt = db.stmt(queryString).asInstanceOf[net.sf.jsqlparser.statement.select.Select]
            val query = db.sql.convert(queryStmt)
            val stats = query.columnNames.flatMap {
              col => Seq( 
                Expectation(col, col), 
                StdDev(col, "DEV_"+col)
              )
            } ++ Seq(Confidence("CONF"))

            val results = db.compiler.compileForStats(
              query,
              stats,
              seeds = Seq(1l, 2l)
            )
            val backendTime = time {
              results.foreach { row => (x = x + 1) }
            }
            results.close()

            (x, backendTime._2)
          }
          println(s"Time:${totalTime} seconds (${backendTime} seconds reading $rows results);  <- Query $idx: \n$queryString")
          rows must be_>(0)
        }
    }
  }

}