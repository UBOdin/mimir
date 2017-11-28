package mimir.gprom.algebra

import org.gprom.jdbc.jna.GProMWrapper
import org.specs2.specification._
import org.specs2.specification.core.Fragments

import mimir._
import mimir.algebra._
import mimir.sql._
import mimir.test._
import net.sf.jsqlparser.statement.select.Select
import mimir.exec.Compiler

object OperatorTranslationSpec extends GProMSQLTestSpecification("GProMOperatorTranslation") with BeforeAll with AfterAll {

  args(skipAll = false)
 
  var memctx : com.sun.jna.Pointer = null
  var qmemctx : com.sun.jna.Pointer = null
  
  def beforeAll =
  {
    update("CREATE TABLE R(A integer, B integer)")
    update("INSERT INTO R (A, B) VALUES(1, 1)")
    update("INSERT INTO R (A, B) VALUES(2, 2)")
    update("INSERT INTO R (A, B) VALUES(3, 3)")
    update("INSERT INTO R (A, B) VALUES(4, 4)")
    update("CREATE TABLE T(C integer, D integer)")
    update("INSERT INTO T (C, D) VALUES(1, 4)")
    update("INSERT INTO T (C, D) VALUES(2, 3)")
    update("INSERT INTO T (C, D) VALUES(3, 2)")
    update("INSERT INTO T (C, D) VALUES(4, 1)")
    
    update("CREATE TABLE Q(E varchar, F varchar)")
    update("INSERT INTO Q (E, F) VALUES(1, 4)")
    update("INSERT INTO Q (E, F) VALUES(2, 1)")
    update("INSERT INTO Q (E, F) VALUES(3, 2)")
    update("INSERT INTO Q (E, F) VALUES(4, 1)")
    
    memctx = GProMWrapper.inst.gpromCreateMemContext()
    qmemctx = GProMWrapper.inst.createMemContextName("QUERY_MEM_CONTEXT")
    
  }
  
  def afterAll = {
    GProMWrapper.inst.gpromFreeMemContext(qmemctx)
    GProMWrapper.inst.gpromFreeMemContext(memctx)
    GProMWrapper.inst.shutdown()
  }

  sequential 
  "The GProM - Mimir Operator Translator" should {
    "Compile Determinism for Projections" >> {
      update("""
          CREATE LENS TIQ
            AS SELECT * FROM Q
          WITH TYPE_INFERENCE(.7)
        """);
      update("""
          CREATE LENS CQ
            AS SELECT * FROM TIQ
          WITH COMMENT(COMMENT(F,'The values are uncertain'))
        """);
      val table = db.table("CQ")
      val (oper, colDet, rowDet) = OperatorTranslation.compileTaintWithGProM(table) 
      colDet.toSeq.length must be equalTo 3
    }
    
    "Compile Determinism for Aggregates" >> {
      val statements = db.parse("select COUNT(COMMENT_ARG_0) from CQ")
      val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
      val (oper, colDet, rowDet) = OperatorTranslation.compileTaintWithGProM(testOper) 
      colDet.toSeq.length must be equalTo 1
    }
  }
  

  "The GProM - Mimir Operator Translator" should {
    sequential
    //isolated
    Fragments.foreach(1 to 1){ i => 
      sequential
      Fragments.foreach(Seq(
        (s"Queries for Tables - run $i", 
            "SELECT R.A, R.B FROM R" ), 
        (s"Queries for Aliased Tables - run $i", 
            "SELECT S.A, S.B FROM R AS S" ), 
        (s"Queries for Tables with Aliased Attributes - run $i", 
            "SELECT R.A AS P, R.B AS Q FROM R"), 
        (s"Queries for Aliased Tables with Aliased Attributes- run $i", 
            "SELECT S.A AS P, S.B AS Q FROM R AS S"),
        (s"Queries for Tables with Epression Attrinutes - run $i",
            "SELECT R.A + R.B AS Z FROM R"),
        (s"Queries for Aliased Tables with Epression Attrinutes - run $i",
            "SELECT S.A + S.B AS Z FROM R AS S"),
        (s"Queries for Tables with Selection - run $i", 
            "SELECT R.A, R.B FROM R WHERE R.A = R.B" ), 
        (s"Queries for Aliased Tables with Selection - run $i", 
            "SELECT S.A, S.B FROM R AS S WHERE S.A = S.B" ), 
        (s"Queries for Tables with Aliased Attributes with Selection - run $i", 
            "SELECT R.A AS P, R.B AS Q FROM R WHERE R.A = R.B"), 
        (s"Queries for Aliased Tables with Aliased Attributes with Selection- run $i", 
            "SELECT S.A AS P, S.B AS Q FROM R AS S WHERE S.A = S.B"), 
        (s"Queries for Tables with Epression Attrinutes with Selection- run $i",
            "SELECT R.A + R.B AS Z FROM R WHERE R.A = R.B"),
        (s"Queries for Aliased Tables with Epression Attrinutes with Selection- run $i",
            "SELECT S.A + S.B AS Z FROM R AS S WHERE S.A = S.B"),
        (s"Queries for Aliased Tables with Joins with Aliased Attributes - run $i", 
            "SELECT S.A AS P, U.C AS Q FROM R AS S JOIN T AS U ON S.A = U.C"),
        (s"Queries for Tables with Aggregates - run $i",
            "SELECT SUM(R.B), COUNT(R.B) FROM R"),
        (s"Queries for Aliased Tables with Aggregates - run $i",
            "SELECT SUM(RB.B), COUNT(RB.B) FROM R RB"),
        (s"Queries for Aliased Tables with Aggregates with Aliased Attributes - run $i",
            "SELECT SUM(RB.B) AS SB, COUNT(RB.B) AS CB FROM R RB"),
        (s"Queries for Aliased Tables with Aggregates with Aliased Attributes Containing Expressions - run $i",
            "SELECT SUM(RB.B + RB.B) AS SAB, COUNT(RB.B) AS CB FROM R RB"),
        (s"Queries for Aliased Tables with Aggregates with Expressions of Aggregates with Aliased Attributes Containing Expressions - run $i",
            "SELECT SUM(RB.A + RB.B) + SUM(RB.A + RB.B) AS SAB, COUNT(RB.B) AS CB FROM R RB")
        ).zipWithIndex){
        daq =>  { 
            //{createGProMMemoryContext(daq)}
            {translateOperatorsFromMimirToGProM(daq)}
            {translateOperatorsFromGProMToMimir(daq)}
            {translateOperatorsFromMimirToGProMToMimir(daq)}
            {translateOperatorsFromGProMToMimirToGProM(daq)}
            {translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL(daq)}
            //{freeGProMMemoryContext(daq)}
        }
      }
    }
  }
  
  def createGProMMemoryContext(descAndQuery : ((String, String), Int)) = s"Create GProM Memory Context for: ${descAndQuery._2} ${descAndQuery._1._1}" >> {
    memctx = GProMWrapper.inst.gpromCreateMemContext()
    qmemctx = GProMWrapper.inst.createMemContextName("QUERY_MEM_CONTEXT")
    (qmemctx != null) must be equalTo true
    
  }
  
  def freeGProMMemoryContext(descAndQuery : ((String, String), Int)) = s"Free GProM Memory Context for: ${descAndQuery._2} ${descAndQuery._1._1}" >> {
    GProMWrapper.inst.gpromFreeMemContext(qmemctx)
      GProMWrapper.inst.gpromFreeMemContext(memctx)
      memctx = null
      qmemctx = null
    memctx must be equalTo null
  }
  
  def translateOperatorsFromMimirToGProM(descAndQuery : ((String, String), Int)) =  s"Translate Operators from Mimir to GProM for: ${descAndQuery._2} ${descAndQuery._1._1}" >> {
      org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized {
         val queryStr = descAndQuery._1._2 
         val statements = db.parse(queryStr)
         val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
         gp.metadataLookupPlugin.setOper(testOper)
         val gpromNode = OperatorTranslation.mimirOperatorToGProMList(testOper)
         gpromNode.write()
         //val memctx = GProMWrapper.inst.gpromCreateMemContext() 
         val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
         val gpromNode2 = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
         val nodeStr2 = GProMWrapper.inst.gpromNodeToString(gpromNode2.getPointer())
         //GProMWrapper.inst.gpromFreeMemContext(memctx)
         val translatedNodeStr = nodeStr.replaceAll("0x[a-zA-Z0-9]+", "") 
         val actualNodeStr = nodeStr2.replaceAll("0x[a-zA-Z0-9]+", "")
         val ret = translatedNodeStr must be equalTo actualNodeStr or 
           {
             val resQuery = GProMWrapper.inst.gpromOperatorModelToQuery(gpromNode.getPointer)
             getQueryResultsBackend(resQuery) must be equalTo getQueryResultsBackend(queryStr)
           }
         ret
      }
    }
  
  def translateOperatorsFromGProMToMimir(descAndQuery : ((String, String), Int)) =  s"Translate Operators from GProM to Mimir for: ${descAndQuery._2} ${descAndQuery._1._1}" >> {
       org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized {
         val queryStr = descAndQuery._1._2 
         val statements = db.parse(queryStr)
         val testOper2 = db.sql.convert(statements.head.asInstanceOf[Select])
         var operStr2 = testOper2.toString()
         //val memctx = GProMWrapper.inst.gpromCreateMemContext()
         val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
         val testOper = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode, null)
         var operStr = testOper.toString()
         //GProMWrapper.inst.gpromFreeMemContext(memctx)
         val ret = operStr must be equalTo operStr2 or 
           {
             operStr2 = totallyOptimize(testOper2).toString()
             operStr = totallyOptimize(testOper).toString()
             operStr must be equalTo operStr2
           } or 
             {
               getQueryResultsBackend(testOper) must be equalTo getQueryResultsBackend(queryStr)
             }
           ret
       }
    }
    
    def translateOperatorsFromMimirToGProMToMimir(descAndQuery : ((String, String), Int)) =  s"Translate Operators from Mimir to GProM to Mimir for: ${descAndQuery._2} ${descAndQuery._1._1}" >> {
         org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized {
           val queryStr = descAndQuery._1._2
           val statements = db.parse(queryStr)
           val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
           var operStr = testOper.toString()
           val gpromNode = OperatorTranslation.mimirOperatorToGProMList(testOper)
           gpromNode.write()
           //val memctx = GProMWrapper.inst.gpromCreateMemContext() 
           val testOper2 = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode, null)
           var operStr2 = testOper2.toString()
           //GProMWrapper.inst.gpromFreeMemContext(memctx)
           val ret = operStr must be equalTo operStr2 or 
             {
               operStr2 = totallyOptimize(testOper2).toString()
               operStr = totallyOptimize(testOper).toString()
               operStr must be equalTo operStr2
             } or 
               {
                 getQueryResultsBackend(testOper) must be equalTo getQueryResultsBackend(queryStr)
               }
             ret
         }
    }
    
    def translateOperatorsFromGProMToMimirToGProM(descAndQuery : ((String, String), Int)) =  s"Translate Operators from GProM to Mimir To GProM for: ${descAndQuery._2} ${descAndQuery._1._1}" >> {
       org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized {
         val queryStr = descAndQuery._1._2 
         //val memctx = GProMWrapper.inst.gpromCreateMemContext() 
         val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
         val testOper = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode, null)
         val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
         //GProMWrapper.inst.gpromFreeMemContext(memctx)
         val gpromNode2 = OperatorTranslation.mimirOperatorToGProMList(testOper)
         gpromNode2.write()
         //GProMWrapper.inst.gpromCreateMemContext() 
         val nodeStr2 = GProMWrapper.inst.gpromNodeToString(gpromNode2.getPointer())
         //GProMWrapper.inst.gpromFreeMemContext(memctx)
         val translatedNodeStr = nodeStr2.replaceAll("0x[a-zA-Z0-9]+", "") 
         val actualNodeStr = nodeStr.replaceAll("0x[a-zA-Z0-9]+", "")
         val ret = translatedNodeStr must be equalTo actualNodeStr or 
           {
             val resQuery = GProMWrapper.inst.gpromOperatorModelToQuery(gpromNode.getPointer).replaceAll("(AS\\s+[a-zA-Z]+)\\(([a-zA-Z0-9,\\s]+)\\)", "$1_$2")
             getQueryResultsBackend(resQuery) must be equalTo getQueryResultsBackend(queryStr)
           } 
         ret
       }
    }

    
    def translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL(descAndQuery : ((String, String), Int)) =  s"Translate Operators not an order of magnitude slower Than Rewriting SQL for: ${descAndQuery._2} ${descAndQuery._1._1}" >> {
       org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized {
         val queryStr = descAndQuery._1._2 
         val statements = db.parse(queryStr)
         val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
         
         val timeForRewriteThroughOperatorTranslation = time {
           val gpromNode = OperatorTranslation.mimirOperatorToGProMList(testOper)
           gpromNode.write()
           //val smemctx = GProMWrapper.inst.gpromCreateMemContext() 
           val gpromNode2 = GProMWrapper.inst.provRewriteOperator(gpromNode.getPointer())
           val testOper2 = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode2, null)
           val operStr = ""//testOper2.toString()
           //GProMWrapper.inst.gpromFreeMemContext(smemctx)
           operStr
        }
         
        val timeForRewriteThroughSQL = time {
           //val sqlToRewrite = db.ra.convert(testOper)
           val sqlRewritten = GProMWrapper.inst.gpromRewriteQuery(queryStr+";")
           /*val statements2 = db.parse(sqlRewritten)
           val testOper2 = db.sql.convert(statements.head.asInstanceOf[Select])
           testOper2.toString()*/""
        }
         
         //timeForRewriteThroughOperatorTranslation._1 must be equalTo timeForRewriteThroughSQL._1
         //println(s"via SQL: ${timeForRewriteThroughSQL._2} via RA: ${timeForRewriteThroughOperatorTranslation._2}")
         val ret = (timeForRewriteThroughOperatorTranslation._2 should be lessThan timeForRewriteThroughSQL._2) or (timeForRewriteThroughOperatorTranslation._2 should be lessThan (timeForRewriteThroughSQL._2*10))
         ret
       }
    }
    
    def time[F](anonFunc: => F): (F, Long) = {  
      val tStart = System.nanoTime()
      val anonFuncRet = anonFunc  
      val tEnd = System.nanoTime()
      (anonFuncRet, tEnd-tStart)
    }
    
    def getQueryResultsBackend(oper : mimir.algebra.Operator) : String =  {
      getQueryResultsBackend(db.ra.convert(oper).toString())
    }
    
    def getQueryResultsBackend(query:String) : String =  {
      val ress = db.backend.execute(query)
      val resmd = ress.getMetaData();
      var i = 1;
      var row = ""
      var resStr = ""
      while(ress.next()){
        i = 1;
        row = ""
        while(i<=resmd.getColumnCount()){
          row += ress.getString(i) + ", ";
          i+=1;
        }
        resStr += row + "\n"
      }
      resStr
    }
    
    def getQueryResults(oper: Operator) : String = {
      db.query(oper)( resIter => resIter.schema.map(f => f._1).mkString("", ",", "\n") + resIter.toList.map(row => row.tuple.map(cell => cell.toString()).mkString( "," )).mkString("\n"))
    }
    
    def totallyOptimize(oper : mimir.algebra.Operator) : mimir.algebra.Operator = {
      val preOpt = oper.toString() 
      val postOptOper = db.compiler.optimize(oper)
      val postOpt = postOptOper.toString() 
      if(preOpt.equals(postOpt))
        postOptOper
      else
        totallyOptimize(postOptOper)
    }
}