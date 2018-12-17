package mimir.algebra.gprom

import org.gprom.jdbc.jna.GProMWrapper
import org.specs2.specification._
import org.specs2.specification.core.Fragments

import mimir._
import mimir.algebra._
import mimir.sql._
import mimir.test._
import net.sf.jsqlparser.statement.select.Select
import mimir.exec.Compiler
import mimir.util.LoggerUtils
import java.io.File
import mimir.util.LoadCSV
import mimir.provenance.Provenance

object OperatorTranslationSpec extends GProMSQLTestSpecification("GProMdb.gpromTranslator") with BeforeAll with AfterAll {

  args(skipAll = false)
 
  var memctx : com.sun.jna.Pointer = null
  var qmemctx : com.sun.jna.Pointer = null
  var provOp : Operator = null
  
  def beforeAll =
  {
    
    LoadCSV.handleLoadTableRaw(db, "R", Some(Seq(("A", TInt()), ("B", TInt()))), new File("test/data/gprom_r.csv"),  Map("DELIMITER" -> ",","ignoreLeadingWhiteSpace"->"true","ignoreTrailingWhiteSpace"->"true", "mode" -> /*"PERMISSIVE"*/"DROPMALFORMED", "header" -> "false") )
    LoadCSV.handleLoadTableRaw(db, "T", Some(Seq(("C", TInt()), ("D", TInt()))), new File("test/data/gprom_t.csv"),  Map("DELIMITER" -> ",","ignoreLeadingWhiteSpace"->"true","ignoreTrailingWhiteSpace"->"true", "mode" -> /*"PERMISSIVE"*/"DROPMALFORMED", "header" -> "false") )
    LoadCSV.handleLoadTableRaw(db, "Q", Some(Seq(("E", TString()), ("F", TString()))), new File("test/data/gprom_q.csv"),  Map("DELIMITER" -> ",","ignoreLeadingWhiteSpace"->"true","ignoreTrailingWhiteSpace"->"true", "mode" -> /*"PERMISSIVE"*/"DROPMALFORMED", "header" -> "false") )
    //loadCSV("R",Seq(("A", "int"), ("B", "int")), new File("test/data/gprom_r.csv"))
    //loadCSV("T",Seq(("C", "int"), ("D", "int")), new File("test/data/gprom_t.csv"))
    //loadCSV("Q",Seq(("E", "varchar"), ("F", "varchar")), new File("test/data/gprom_q.csv"))
    
    //println(Seq("R","T","Q").map( table => query(db.table(table)){ res => res.toList.map( _.tuple.mkString(",") ).mkString(res.schema.unzip._1.mkString(table+"\n", ",", "\n"),"\n","\n") } ).mkString("--------------------------------------\n"))
    
    memctx = GProMWrapper.inst.gpromCreateMemContext()
    qmemctx = GProMWrapper.inst.createMemContextName("QUERY_MEM_CONTEXT")
    
    update("""
          CREATE LENS MVQ
            AS SELECT * FROM Q
          WITH MISSING_VALUE(E)
        """);
      update("""
          CREATE LENS CQ
            AS SELECT * FROM MVQ
          WITH COMMENT(COMMENT(F,'The values are uncertain'))
        """);
  }
  
  def afterAll = {
    //GProMWrapper.inst.gpromFreeMemContext(qmemctx)
    //GProMWrapper.inst.gpromFreeMemContext(memctx)
    //GProMWrapper.inst.shutdown()
  }

  sequential 
  "The GProM - Mimir Operator Translator" should {
    sequential 
    "Compile Provenance for Projections" >> {
      /*LoggerUtils.debug(
				"mimir.algebra.gprom.db.gpromTranslator"
			){*/
        val table = db.table("CQ")
        val (oper, provCols) = db.gpromTranslator.compileProvenanceWithGProM(table) 
        provOp = oper;
        query(table){ 
  				_.toSeq.map { _.provenance.asString } must contain(
  					"1",
  					"2",
  					"3",
  					"4"
  				)
  			}
        provCols must contain("PROV_Q_MIMIR__ROWID")
      //}
    }
    
    "Compile Determinism for Projections" >> { 
      val (oper, colDet, rowDet) = db.gpromTranslator.compileTaintWithGProM(provOp) 
      val table = db.table("CQ")
      query(table){ 
				_.toList.map( row => {
				  (row.tupleSchema.map{ _._1 }.map{ row.isColDeterministic(_) } :+ row.isDeterministic()).toSeq
				} )  
			}.toList must be equalTo scala.collection.immutable.List(
					Seq(true, true, false, true), 
					Seq(false, true, false, true), 
					Seq(true, true, false, true), 
					Seq(true, true, false, true)
				)
      
      colDet must contain(
          ("E",Var("MIMIR_COL_DET_E")), 
          ("F",Var("MIMIR_COL_DET_F")), 
          ("COMMENT_ARG_0", Var("MIMIR_COL_DET_COMMENT_ARG_0")), 
          ("PROV_Q_MIMIR__ROWID", Var("MIMIR_COL_DET_PROV_Q_MIMIR__ROWID"))  
        )
      
      rowDet must be equalTo Var("MIMIR_COL_DET_R")
    }
    
    "Compile Provenance for Aggregates" >> {
      //LoggerUtils.debug(
			//	"mimir.algebra.gprom.db.gpromTranslator"
			//){
        val statements = db.parse("select COUNT(COMMENT_ARG_0) from CQ")
        val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
        val (oper, provCols) = db.gpromTranslator.compileProvenanceWithGProM(testOper) 
        provOp = oper;
        
        provCols must contain("PROV_Q_MIMIR__ROWID")
        
        query(testOper){ 
  				_.toSeq.map { _.provenance.asString } must contain(
  					"1",
  					"2",
  					"3",
  					"4"
  				)
  			} 
      //}
    }
    
    "Compile Determinism for Aggregates" >> {
      val (oper, colDet, rowDet) = db.gpromTranslator.compileTaintWithGProM(provOp) 
      val statements = db.parse("select COUNT(COMMENT_ARG_0) from CQ")
      val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
      query(testOper){ 
				_.toList.map( row => {
				  (row.tupleSchema.map{ _._1 }.map{ row.isColDeterministic(_) } :+ row.isDeterministic()).toSeq
				} ) 
			}.toList must be equalTo scala.collection.immutable.List(
					Seq(false, true), 
					Seq(false, true), 
					Seq(false, true), 
				  Seq(false, true)
				)
      
      colDet must contain(
          ("COUNT",Var("MIMIR_COL_DET_COUNT")), 
          ("PROV_Q_MIMIR__ROWID",Var("MIMIR_COL_DET_PROV_Q_MIMIR__ROWID"))
        )
      
      rowDet must be equalTo Var("MIMIR_COL_DET_R")
    }

  "Translate for Simple Queries" >> {
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
        /*(s"Queries for Aliased Tables with Joins with Aliased Attributes - run $i", //failing
            "SELECT S.A AS P, U.C AS Q FROM R AS S JOIN T AS U ON S.A = U.C"),*/
        (s"Queries for Tables with Aggregates - run $i",
            "SELECT SUM(R.B) AS SUM_B, COUNT(R.B) AS COUNT_B FROM R"),
        (s"Queries for Aliased Tables with Aggregates - run $i",
            "SELECT SUM(RB.B) AS SUM_B, COUNT(RB.B) AS COUNT_B FROM R RB"),
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
         val testOper = db.select(queryStr)
         //gp.metadataLookupPlugin.setOper(testOper)
         val gpromNode = db.gpromTranslator.mimirOperatorToGProMList(testOper)
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
             val optOper = GProMWrapper.inst.optimizeOperatorModel(gpromNode.getPointer)
             val resQuery = GProMWrapper.inst.gpromOperatorModelToQuery(optOper.getPointer)
             getQueryResultsBackend(resQuery) must be equalTo getQueryResultsBackend(queryStr)
           }
         ret
      }
    }
  
  def translateOperatorsFromGProMToMimir(descAndQuery : ((String, String), Int)) =  s"Translate Operators from GProM to Mimir for: ${descAndQuery._2} ${descAndQuery._1._1}" >> {
       org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized {
         val queryStr = descAndQuery._1._2 
         val testOper2 = db.select(queryStr)
         var operStr2 = testOper2.toString()
         //val memctx = GProMWrapper.inst.gpromCreateMemContext()
         val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
         val testOper = db.gpromTranslator.gpromStructureToMimirOperator(0, gpromNode, null)
         var operStr = testOper.toString()
         //GProMWrapper.inst.gpromFreeMemContext(memctx)
         val ret = operStr must be equalTo operStr2 or 
           {
             operStr2 = totallyOptimize(testOper2).toString()
             operStr = totallyOptimize(testOper).toString()
             operStr must be equalTo operStr2
           } or 
             {
               val optOper = GProMWrapper.inst.optimizeOperatorModel(gpromNode.getPointer)
               val resOp = db.gpromTranslator.gpromStructureToMimirOperator(0, optOper, null)
               getQueryResultsBackend(resOp.removeColumn(Provenance.rowidColnameBase)) must be equalTo getQueryResultsBackend(queryStr)
             }
           ret
       }
    }
    
    def translateOperatorsFromMimirToGProMToMimir(descAndQuery : ((String, String), Int)) =  s"Translate Operators from Mimir to GProM to Mimir for: ${descAndQuery._2} ${descAndQuery._1._1}" >> {
         org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized {
           val queryStr = descAndQuery._1._2
           val testOper = db.select(queryStr)
           var operStr = testOper.toString()
           val gpromNode = db.gpromTranslator.mimirOperatorToGProMList(testOper)
           gpromNode.write()
           //val memctx = GProMWrapper.inst.gpromCreateMemContext() 
           val testOper2 = db.gpromTranslator.gpromStructureToMimirOperator(0, gpromNode, null)
           var operStr2 = testOper2.toString()
           //GProMWrapper.inst.gpromFreeMemContext(memctx)
           val ret = operStr must be equalTo operStr2 or 
             {
               operStr2 = totallyOptimize(testOper2).toString()
               operStr = totallyOptimize(testOper).toString()
               operStr must be equalTo operStr2
             } or 
               {
                 val optOper = GProMWrapper.inst.optimizeOperatorModel(gpromNode.getPointer)
                 val resOp = db.gpromTranslator.gpromStructureToMimirOperator(0, optOper, null)
                 getQueryResultsBackend(resOp.removeColumn(Provenance.rowidColnameBase)) must be equalTo getQueryResultsBackend(queryStr)
               }
             ret
         }
    }
    
    def translateOperatorsFromGProMToMimirToGProM(descAndQuery : ((String, String), Int)) =  s"Translate Operators from GProM to Mimir To GProM for: ${descAndQuery._2} ${descAndQuery._1._1}" >> {
       org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized {
         val queryStr = descAndQuery._1._2 
         //val memctx = GProMWrapper.inst.gpromCreateMemContext() 
         val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
         val testOper = db.gpromTranslator.gpromStructureToMimirOperator(0, gpromNode, null)
         val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
         //GProMWrapper.inst.gpromFreeMemContext(memctx)
         val gpromNode2 = db.gpromTranslator.mimirOperatorToGProMList(testOper)
         gpromNode2.write()
         //GProMWrapper.inst.gpromCreateMemContext() 
         val nodeStr2 = GProMWrapper.inst.gpromNodeToString(gpromNode2.getPointer())
         //GProMWrapper.inst.gpromFreeMemContext(memctx)
         val translatedNodeStr = nodeStr2.replaceAll("0x[a-zA-Z0-9]+", "") 
         val actualNodeStr = nodeStr.replaceAll("0x[a-zA-Z0-9]+", "")
         val ret = translatedNodeStr must be equalTo actualNodeStr or 
           {
             val optOper = GProMWrapper.inst.optimizeOperatorModel(gpromNode.getPointer)
             val resQuery = GProMWrapper.inst.gpromOperatorModelToQuery(optOper.getPointer).replaceAll("(AS\\s+[a-zA-Z]+)\\(([a-zA-Z0-9,\\s]+)\\)", "$1_$2")
             getQueryResultsBackend(resQuery) must be equalTo getQueryResultsBackend(queryStr)
           } 
         ret
       }
    }

    
    def translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL(descAndQuery : ((String, String), Int)) =  s"Translate Operators not an order of magnitude slower Than Rewriting SQL for: ${descAndQuery._2} ${descAndQuery._1._1}" >> {
       org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized {
         val queryStr = descAndQuery._1._2 
         val testOper = db.select(queryStr)
          
         val timeForRewriteThroughOperatorTranslator = time {
           val gpromNode = db.gpromTranslator.mimirOperatorToGProMList(testOper)
           gpromNode.write()
           //val smemctx = GProMWrapper.inst.gpromCreateMemContext() 
           val gpromNode2 = GProMWrapper.inst.provRewriteOperator(gpromNode.getPointer())
           val testOper2 = db.gpromTranslator.gpromStructureToMimirOperator(0, gpromNode2, null)
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
         
         //timeForRewriteThroughdb.gpromTranslator._1 must be equalTo timeForRewriteThroughSQL._1
         //println(s"via SQL: ${timeForRewriteThroughSQL._2} via RA: ${timeForRewriteThroughdb.gpromTranslator._2}")
         val ret = (timeForRewriteThroughOperatorTranslator._2 should be lessThan timeForRewriteThroughSQL._2) or (timeForRewriteThroughOperatorTranslator._2 should be lessThan (timeForRewriteThroughSQL._2*10))
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
      try{
        db.query(oper)(results => {
          results.toList.map(row => {
            row.tuple.mkString(",")
          }).mkString( results.schema.map(_._1).mkString("",",","\n"), "\n", "")
        })
      }catch {
        case t: Throwable => throw new Exception(s"Error getting query results: query: \n$oper", t)
      }
    }
    
    def getQueryResultsBackend(query:String) : String =  {
      val oper = db.select(query)
      getQueryResultsBackend(oper)
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