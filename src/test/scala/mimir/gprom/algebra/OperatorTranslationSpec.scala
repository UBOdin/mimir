package mimir.gprom.algebra

import mimir.parser.{MimirJSqlParser}
import org.specs2.mutable._
import org.specs2.specification._

import mimir._
import mimir.parser._
import mimir.algebra._
import mimir.sql._
import mimir.test._
import org.gprom.jdbc.jna.GProMWrapper
import org.gprom.jdbc.jna.GProMNode
import net.sf.jsqlparser.statement.select.Select
import org.specs2.specification.core.Fragments

object OperatorTranslationSpec extends GProMSQLTestSpecification("GProMOperatorTranslation") with BeforeAll with AfterAll {
 
  var memctx : com.sun.jna.Pointer = null
  
  def convert(x: String) = db.ra.convert(oper(x)).toString

  def beforeAll =
  {
    update("CREATE TABLE R(A integer, B integer)")
    update("CREATE TABLE T(C integer, D integer)")
    memctx = GProMWrapper.inst.gpromCreateMemContext()
  }
  
  def afterAll = {
    GProMWrapper.inst.gpromFreeMemContext(memctx)
  }
  

  "The GProM - Mimir Operator Translator" should {

    
    sequential
    Fragments.foreach(1 to 5){ i => 
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
        (s"Queries for Aliased Tables with Aggragates with Aliased Attributes - run $i",
            "SELECT SUM(RB.INT_COL_B) AS SB, COUNT(RB.INT_COL_B) AS CB FROM TEST_B_RAW RB")
        )){
        daq =>  {
          {translateOperatorsFromMimirToGProM(daq)}
          {translateOperatorsFromGProMToMimir(daq)}
          {translateOperatorsFromMimirToGProMToMimir(daq)}
          {translateOperatorsFromGProMToMimirToGProM(daq)}
          {translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL(daq)}
        }
      }
    }
  }
  
  def translateOperatorsFromMimirToGProM(descAndQuery : (String, String)) =  s"Translate Operators from Mimir to GProM for ${descAndQuery._1}" >> {
         val queryStr = descAndQuery._2 
         val statements = db.parse(queryStr)
         val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
         val gpromNode = OperatorTranslation.mimirOperatorToGProMList(testOper)
         gpromNode.write()
         //val memctx = GProMWrapper.inst.gpromCreateMemContext() 
         val nodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
         val gpromNode2 = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
         val nodeStr2 = GProMWrapper.inst.gpromNodeToString(gpromNode2.getPointer())
         //GProMWrapper.inst.gpromFreeMemContext(memctx)
         nodeStr.replaceAll("0x[a-zA-Z0-9]+", "") must be equalTo nodeStr2.replaceAll("0x[a-zA-Z0-9]+", "")
    }
  
  def translateOperatorsFromGProMToMimir(descAndQuery : (String, String)) =  s"Translate Operators from GProM to Mimir for ${descAndQuery._1}" >> {
         val queryStr = descAndQuery._2 
         val statements = db.parse(queryStr)
         val testOper2 = db.sql.convert(statements.head.asInstanceOf[Select])
         val operStr2 = testOper2.toString()
         //val memctx = GProMWrapper.inst.gpromCreateMemContext()
         val gpromNode = GProMWrapper.inst.rewriteQueryToOperatorModel(queryStr+";")
         val testOper = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode, null)
         val operStr = testOper.toString()
         //GProMWrapper.inst.gpromFreeMemContext(memctx)
         operStr must be equalTo operStr2
    }
    
    def translateOperatorsFromMimirToGProMToMimir(descAndQuery : (String, String)) =  s"Translate Operators from Mimir to GProM to Mimir for ${descAndQuery._1}" >> {
         val queryStr = descAndQuery._2
         val statements = db.parse(queryStr)
         val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
         val operStr = testOper.toString()
         val gpromNode = OperatorTranslation.mimirOperatorToGProMList(testOper)
         gpromNode.write()
         //val memctx = GProMWrapper.inst.gpromCreateMemContext() 
         val testOper2 = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode, null)
         val operStr2 = testOper2.toString()
         //GProMWrapper.inst.gpromFreeMemContext(memctx)
         operStr must be equalTo operStr2
    }
    
    def translateOperatorsFromGProMToMimirToGProM(descAndQuery : (String, String)) =  s"Translate Operators from GProM to Mimir To GProM for ${descAndQuery._1}" >> {
         val queryStr = descAndQuery._2 
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
         nodeStr.replaceAll("0x[a-zA-Z0-9]+", "") must be equalTo nodeStr2.replaceAll("0x[a-zA-Z0-9]+", "")
    }

    
    def translateOperatorsFromMimirToGProMForRewriteFasterThanThroughSQL(descAndQuery : (String, String)) =  s"Translate Operators from Mimir to GProM for Rewrite Faster Than SQL for ${descAndQuery._1}" >> {
         val queryStr = descAndQuery._2 
         val statements = db.parse(queryStr)
         val testOper = db.sql.convert(statements.head.asInstanceOf[Select])
         
         val timeForRewriteThroughOperatorTranslation = time {
           val gpromNode = OperatorTranslation.mimirOperatorToGProMList(testOper)
           gpromNode.write()
           //val memctx = GProMWrapper.inst.gpromCreateMemContext() 
           val gpromNode2 = GProMWrapper.inst.provRewriteOperator(gpromNode.getPointer())
           val testOper2 = OperatorTranslation.gpromStructureToMimirOperator(0, gpromNode2, null)
           val operStr = testOper2.toString()
           //GProMWrapper.inst.gpromFreeMemContext(memctx)
           operStr
        }
         
        val timeForRewriteThroughSQL = time {
           //val sqlToRewrite = db.ra.convert(testOper)
           val sqlRewritten = GProMWrapper.inst.gpromRewriteQuery(queryStr+";")
           val statements2 = db.parse(sqlRewritten)
           val testOper2 = db.sql.convert(statements.head.asInstanceOf[Select])
           testOper2.toString()
        }
         
         timeForRewriteThroughOperatorTranslation._1 must be equalTo timeForRewriteThroughSQL._1
         timeForRewriteThroughOperatorTranslation._2 should be lessThan timeForRewriteThroughSQL._2 
    }
    
    def time[F](anonFunc: => F): (F, Long) = {  
      val tStart = System.nanoTime()
      val anonFuncRet = anonFunc  
      val tEnd = System.nanoTime()
      (anonFuncRet, tEnd-tStart)
    }
}