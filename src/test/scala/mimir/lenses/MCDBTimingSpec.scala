package mimir.lenses

import java.io._
import org.specs2.specification._

import mimir.algebra._
import mimir.util._
import mimir.ctables.{VGTerm}
import mimir.optimizer.{InlineVGTerms,InlineProjections}
import mimir.test.{SQLTestSpecification, MCDBWorkload}
import mimir.models._
import org.specs2.specification.core.Fragments

object MCDBTimingSpec
  extends SQLTestSpecification("MCDBTiming", Map("reset" -> "NO", "inline" -> "YES"))
  with BeforeAll
{

  sequential

  args(skipAll = !MCDBWorkload.isDownloaded)

  def beforeAll =
  {
     update("DELETE FROM MIMIR_MODEL_OWNERS")
     update("DELETE FROM MIMIR_MODELS")
     update("DELETE FROM MIMIR_VIEWS")
  }

  val relevantTables = Set(
     "customer",
     "supplier",
     "lineitem",
     "nation",  
     "orders",  
     "part",    
     "partsupp",
     "region"  
  )

  "The MCDB Workload Timing" should {

    sequential
    Fragments.foreach(1 to 1){ i =>
      sequential
      Fragments.foreach(
        MCDBWorkload.attributes.
          filter( x => relevantTables(x._1) )
      ){ createTable(_, s"_run_$i") }
      
      {q1(s"run $i",1500)}
      {q2(s"run $i",2160)}
    }

  }

  def createTable(tableInfo:(String, String, Double), tableSuffix: String = "_cleaned") =  
  {
    val (baseTable, ddl, timeout) = tableInfo
    
    s"Create table: $baseTable" >> {
      if(!db.tableExists(baseTable)){
        update(ddl)
        LoadCSV.handleLoadTable(db, 
          baseTable, 
          new File(s"test/mcdb/${baseTable}.tbl"), 
          Map(
            "HEADER" -> "NO",
            "DELIMITER" -> "|"
          )
        )
      }
      db.tableExists(baseTable) must beTrue
    }
  }

 def q1(queryAndTime : (String, Double)) =  s"Q1 faster than timeout: ${queryAndTime._2} : ${queryAndTime._1}" >> {
     val updSql1 = """
          CREATE TABLE FROM_JAPAN AS
          SELECT *
          FROM nation, supplier, lineitem, partsupp
          WHERE n_name='JAPAN' AND s_suppkey=ps_suppkey AND
            ps_partkey=l_partkey AND ps_suppkey=l_suppkey
            AND n_nationkey = s_nationkey
        """
     
     val updSql2 = """    
          CREATE TABLE INCREASE_PER_CUST AS
          SELECT o_custkey AS custkey, SUM(strftime('%Y', o_orderdate)
            -1994.0)/SUM(1995.0-strftime('%Y', o_orderdate)) AS incr
          FROM ORDERS
          WHERE strftime('%Y', o_orderdate)='1994' OR strftime('%Y', o_orderdate)='1995'
          GROUP BY o_custkey
        """
     
     val updSql3 = """  
            CREATE LENS order_increase AS
              SELECT o_orderkey, INCR  
              FROM ORDERS, increase_per_cust 
              WHERE o_custkey=custkey AND
                MIMIRCAST(strftime('%Y', o_orderdate), 3)='1995'
                WITH PICKER(
                  UEXPRS('TRUE','POSSION(INCR)'),
                  PICK_FROM(incr),
                  PICK_AS(new_cnt)
                )
          """
     
     val updSql4 = """
         CREATE VIEW REV_INCREASE AS
           SELECT (l_extendedprice * (1.0 - l_discount) * new_cnt) AS NEWREV,
            (l_extendedprice * (1.0 - l_discount)) AS OLDREV
           FROM order_increase, from_japan
           WHERE l_orderkey=o_orderkey
       """
     
     val querySql = """
          SELECT (NEWREV - OLDREV)
          FROM REV_INCREASE
        """
     
     val timeForQuery = time {
        if(!db.tableExists("FROM_JAPAN"))
        //  db.views.drop("FROM_JAPAN") 
          db.backend.update(updSql1)
        println("created from_japan")
     
        if(!db.tableExists("increase_per_cust".toUpperCase()))
        //  db.views.drop("increase_per_cust".toUpperCase())
          db.backend.update(updSql2)
        println("created increase_per_cust")
     
        //if(db.tableExists("ORDER_INCREASE"))
        //  db.views.drop("ORDER_INCREASE")
        update(updSql3)
        println("created order_increase")
        
        update(updSql4)
        println("created REV_INCREASE")
     
        val r = query(querySql)
        println("queried REV_INCREASE")
        var x = 0
        r.open
        while(r.getNext()){ x += 1 }
        
        println(s"$x Rows ")
     }
     println(s"Time:${timeForQuery._2} seconds <- Query:$updSql1$updSql2$updSql3$querySql ")
     timeForQuery._2 should be lessThan queryAndTime._2
  }
 
 
  def q2(queryAndTime : (String, Double)) =  s"Q2 faster than timeout: ${queryAndTime._2} : ${queryAndTime._1}" >> {
     
    val updSql1 = """
      CREATE TABLE ORDERS_TODAY AS
      SELECT * FROM orders, lineitem
      WHERE o_orderdate=date('1997-10-31') AND o_orderkey=l_orderkey
    """
    
    val updSql2 = """
      CREATE TABLE PARAMS AS
      SELECT AVG(strftime('%s',l_shipdate)-strftime('%s',o_orderdate)) AS ship_mu,
        AVG(strftime('%s',l_receiptdate)-strftime('%s',l_shipdate)) AS arrv_mu,
        STDDEV(strftime('%s',l_shipdate)-strftime('%s',o_orderdate)) AS ship_sigma,
        STDDEV(strftime('%s',l_receiptdate)-strftime('%s',l_shipdate)) AS arrv_sigma,
        l_partkey AS p_partkey
      FROM orders, lineitem
      WHERE o_orderkey=l_orderkey
      GROUP BY l_partkey
     """
    
     val updSql3 = """
      CREATE LENS ship_times AS
        SELECT o_orderkey, ship_mu, ship_sigma FROM params, orders_today WHERE p_partkey=l_partkey
          WITH PICKER(
              UEXPRS('TRUE','GAMMA(SHIP_MU,SHIP_SIGMA)'),
              PICK_FROM(SHIP_MU,SHIP_SIGMA),
              PICK_AS(GAMMA_SHIP)
            )
     """
    
     val updSql4 = """
      CREATE LENS arrv_times AS
        SELECT o_orderkey, arrv_mu, arrv_sigma FROM params, orders_today WHERE p_partkey=l_partkey
          WITH PICKER(
              UEXPRS('TRUE','GAMMA(ARRV_MU,ARRV_SIGMA)'),
              PICK_FROM(ARRV_MU,ARRV_SIGMA),
              PICK_AS(GAMMA_ARRV)
            )
     """
      
     val querySql = """
      SELECT MAX(GAMMA_SHIP+GAMMA_ARRV) 
      FROM ship_times st, arrv_times at 
      WHERE at.o_orderkey = st.o_orderkey
     """
    
     val timeForQuery = time {
        if(!db.tableExists("ORDERS_TODAY"))
          db.backend.update(updSql1)
        println("created orders_today")
     
        if(!db.tableExists("PARAMS"))
          db.backend.update(updSql2)
        println("created params")
     
        update(updSql3)
        println("created ship_times")
        
        update(updSql4)
        println("created arrv_times")
     
        val r = query(querySql)
        println("queried arrv_times and ship_times")
        var x = 0
        r.open
        while(r.getNext()){ x += 1 }
        
        println(s"$x Rows ")
     }
     println(s"Time:${timeForQuery._2} seconds <- Query:$updSql1$updSql2$updSql3$querySql ")
     timeForQuery._2 should be lessThan queryAndTime._2
  }

  def time[F](anonFunc: => F): (F, Double) = {
      val tStart = System.nanoTime()
      val anonFuncRet = anonFunc
      val tEnd = System.nanoTime()
      (anonFuncRet, (tEnd-tStart).toDouble/1000.0/1000.0/1000.0)
    }

}
