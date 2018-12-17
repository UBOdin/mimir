package mimir.demo


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
import mimir.algebra.gprom.OperatorTranslation
import mimir.algebra.gprom.TranslationUtils
import java.io.File
import mimir.util.LoadCSV

object MimirGProMDemo extends GProMSQLTestSpecification("MimirGProMDemo") 
  with BeforeAll 
  with AfterAll {

  var provOp : Operator = null
  
  def beforeAll =
  {
    
    LoadCSV.handleLoadTableRaw(db, "Q", Some(Seq(("E", TString()), ("F", TString()))), new File("test/data/gprom_q.csv"),  Map("DELIMITER" -> ",","ignoreLeadingWhiteSpace"->"true","ignoreTrailingWhiteSpace"->"true", "mode" -> /*"PERMISSIVE"*/"DROPMALFORMED", "header" -> "false") )
    
    update("""
          CREATE LENS MVQ
            AS SELECT * FROM Q
          WITH MISSING_VALUE(E)
        """);
      
    
  }
  
  def afterAll = {
    //GProMWrapper.inst.shutdown()
  }

  sequential 
  "The GProM - Mimir Operator Translator" should {
    "Translate Mimir Operators to GProM Operators" >> {
      transMOpPrint(db.table("MVQ")).replaceAll("\\s","") must be equalTo gpromOpStr.replaceAll("\\s","")
    }
    
    "Translate GProM Operators to Mimir Operators" >> {
      transGOpPrint(db.table("MVQ")) must be equalTo mimirOpStr
    }
    
    "Compile Provenance for Mimir Operators" >> {
      //LoggerUtils.debug(
			//	"mimir.algebra.gprom.OperatorTranslation"
			//){
        val table = db.table("MVQ")
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
    
    "Compile Determinism for Mimir Operators" >> { 
      val (oper, colDet, rowDet) = db.gpromTranslator.compileTaintWithGProM(provOp) 
      val table = db.table("MVQ")
      query(table){ 
				_.toList.map( row => {
				  (row.tupleSchema.map{ _._1 }.map{ row.isColDeterministic(_) } :+ row.isDeterministic()).toSeq
				} )  
			}.toList must be equalTo scala.collection.immutable.List(
					Seq(true, true, true), 
					Seq(false, true, true), 
					Seq(true, true, true), 
					Seq(true, true, true)
				)
      
      colDet must contain(
          ("E",Var("MIMIR_COL_DET_E")), 
          ("F",Var("MIMIR_COL_DET_F")), 
          ("PROV_Q_MIMIR__ROWID", Var("MIMIR_COL_DET_PROV_Q_MIMIR__ROWID"))  
        )
      
      rowDet must be equalTo Var("MIMIR_COL_DET_R")
    }
    
    
    
  }
  
  def transMOpPrint(oper:Operator) : String = {
    org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized{
      val memctx = GProMWrapper.inst.gpromCreateMemContext()
      /*println("--------------mimir Op--------------------")
      println(oper.toString())
      println("------------mimir Op Json-----------------")
      println(mimir.serialization.Json.ofOperator(oper).toString)
      println("------------------------------------------")*/
      val gpromNode = TranslationUtils.scalaListToGProMList(Seq(db.gpromTranslator.mimirOperatorToGProMOperator(oper)))
      gpromNode.write()
      val gpNodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
      .replaceAll("ADDRESS: '[a-z0-9]+'", "ADDRESS: ''")
      .replaceAll("parents: \\[[a-z0-9\\w\\s']+\\]", "parents: []")
      /*println("---------------gprom Op-------------------")
      println(gpNodeStr)
      println("------------------------------------------")*/
      GProMWrapper.inst.gpromFreeMemContext(memctx)
      gpNodeStr
    }
  }
  
  def transGOpPrint(oper:Operator) : String = {
    org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized{
      val memctx = GProMWrapper.inst.gpromCreateMemContext()
      val gpromNode = TranslationUtils.scalaListToGProMList(Seq(db.gpromTranslator.mimirOperatorToGProMOperator(oper)))
      gpromNode.write()
      val gpNodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
      .replaceAll("ADDRESS: '[a-z0-9]+'", "ADDRESS: ''")
      .replaceAll("parents: \\[[a-z0-9\\w\\s']+\\]", "parents: []")
      /*println("---------------gprom Op-------------------")
      println(gpNodeStr)
      println("------------------------------------------")*/
      val opOut = db.gpromTranslator.gpromStructureToMimirOperator(0, gpromNode, null)
      /*println("--------------mimir Op--------------------")
      println(opOut.toString())
      println("------------mimir Op Json-----------------")
      println(mimir.serialization.Json.ofOperator(opOut).toString)
      println("------------------------------------------")*/
      GProMWrapper.inst.gpromFreeMemContext(memctx)
      opOut.toString()
    }
  }
  
  val gpromOpStr = """[{type:'PROJECTION_OPERATOR',ADDRESS:'',parents:[],schema:{type:'SCHEMA',name:"PROJECTION",attrDefs:[{type:'ATTRIBUTE_DEF',dataType:'DT_STRING-2',attrName:"E"},{type:'ATTRIBUTE_DEF',dataType:'DT_STRING-2',attrName:"F"}]},provAttrs:null,properties:null,inputs:[{type:'PROJECTION_OPERATOR',ADDRESS:'',parents:[],schema:{type:'SCHEMA',name:"PROJECTION",attrDefs:[{type:'ATTRIBUTE_DEF',dataType:'DT_STRING-2',attrName:"E"},{type:'ATTRIBUTE_DEF',dataType:'DT_STRING-2',attrName:"F"}]},provAttrs:null,properties:null,inputs:[{type:'PROJECTION_OPERATOR',ADDRESS:'',parents:[],schema:{type:'SCHEMA',name:"PROJECTION",attrDefs:[{type:'ATTRIBUTE_DEF',dataType:'DT_STRING-2',attrName:"Q_E"},{type:'ATTRIBUTE_DEF',dataType:'DT_STRING-2',attrName:"Q_F"}]},provAttrs:null,properties:null,inputs:[{type:'PROJECTION_OPERATOR',ADDRESS:'',parents:[],schema:{type:'SCHEMA',name:"PROJECTION",attrDefs:[{type:'ATTRIBUTE_DEF',dataType:'DT_STRING-2',attrName:"E"},{type:'ATTRIBUTE_DEF',dataType:'DT_STRING-2',attrName:"F"}]},provAttrs:null,properties:null,inputs:[{type:'TABLE_ACCESS_OPERATOR',ADDRESS:'',parents:[],schema:{type:'SCHEMA',name:"Q",attrDefs:[{type:'ATTRIBUTE_DEF',dataType:'DT_STRING-2',attrName:"E"},{type:'ATTRIBUTE_DEF',dataType:'DT_STRING-2',attrName:"F"}]},provAttrs:null,properties:null,inputs:null,asOf:null,tableName:"Q"}],projExprs:[{type:'ATTRIBUTE_REFERENCE',name:"E",fromClauseItem:0,attrPosition:0,outerLevelsUp:0,attrType:'DT_STRING-2'},{type:'ATTRIBUTE_REFERENCE',name:"F",fromClauseItem:0,attrPosition:1,outerLevelsUp:0,attrType:'DT_STRING-2'}]}],projExprs:[{type:'ATTRIBUTE_REFERENCE',name:"E",fromClauseItem:0,attrPosition:0,outerLevelsUp:0,attrType:'DT_STRING-2'},{type:'ATTRIBUTE_REFERENCE',name:"F",fromClauseItem:0,attrPosition:1,outerLevelsUp:0,attrType:'DT_STRING-2'}]}],projExprs:[{type:'ATTRIBUTE_REFERENCE',name:"Q_E",fromClauseItem:0,attrPosition:0,outerLevelsUp:0,attrType:'DT_STRING-2'},{type:'ATTRIBUTE_REFERENCE',name:"Q_F",fromClauseItem:0,attrPosition:1,outerLevelsUp:0,attrType:'DT_STRING-2'}]}],projExprs:[{type:'CASE_EXPR',expr:null,whenClauses:[{type:'CASE_WHEN',when:{type:'FUNCTIONCALL',functionname:"NOT",args:[{type:'IS_NULL_EXPR',expr:{type:'ATTRIBUTE_REFERENCE',name:"E",fromClauseItem:0,attrPosition:0,outerLevelsUp:0,attrType:'DT_STRING-2'}}],isAgg:false},then:{type:'ATTRIBUTE_REFERENCE',name:"E",fromClauseItem:0,attrPosition:0,outerLevelsUp:0,attrType:'DT_STRING-2'}}],elseRes:{type:'CASE_EXPR',expr:null,whenClauses:[{type:'CASE_WHEN',when:{type:'OPERATOR',name:"=",args:[{type:'FUNCTIONCALL',functionname:"UNCERT",args:[{type:'FUNCTIONCALL',functionname:"MIMIR_ENCODED_VGTERM",args:[{type:'CONSTANT',constType:'DT_STRING-2',value:'MVQ:META:E',isNull:false},{type:'CONSTANT',constType:'DT_INT-0',value:0,isNull:false}],isAgg:false}],isAgg:false},{type:'CONSTANT',constType:'DT_STRING-2',value:'SPARKML',isNull:false}]},then:{type:'FUNCTIONCALL',functionname:"UNCERT",args:[{type:'FUNCTIONCALL',functionname:"MIMIR_ENCODED_VGTERM",args:[{type:'CONSTANT',constType:'DT_STRING-2',value:'MVQ:SPARKML:E',isNull:false},{type:'CONSTANT',constType:'DT_INT-0',value:0,isNull:false},{type:'ROWNUMEXPR'},{type:'ATTRIBUTE_REFERENCE',name:"E",fromClauseItem:0,attrPosition:0,outerLevelsUp:0,attrType:'DT_STRING-2'},{type:'ATTRIBUTE_REFERENCE',name:"F",fromClauseItem:0,attrPosition:1,outerLevelsUp:0,attrType:'DT_STRING-2'}],isAgg:false}],isAgg:false}}],elseRes:{type:'CONSTANT',constType:'DT_INT-0',value:null,isNull:true}}},{type:'ATTRIBUTE_REFERENCE',name:"F",fromClauseItem:0,attrPosition:1,outerLevelsUp:0,attrType:'DT_STRING-2'}]}]"""
  
  
val mimirOpStr = """PROJECT[E <= IF NOT(E IS NULL) THEN E ELSE IF  ({{ MVQ:META:E;0[][] }}='SPARKML')  THEN {{ MVQ:SPARKML:E;0[MIMIR_ROWID][E, F] }} ELSE NULL END END, F <= F](
  PROJECT[E <= Q_E, F <= Q_F](
    PROJECT[Q_E <= E, Q_F <= F](
      PROJECT[E <= E, F <= F](
        Q(E:varchar, F:varchar // MIMIR_ROWID:rowid <- ROWID)
      )
    )
  )
)"""  

}