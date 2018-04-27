package mimir.algebra.gprom

import org.gprom.jdbc.jna._
import mimir.algebra._
import com.typesafe.scalalogging.slf4j.LazyLogging

object TranslationUtils extends LazyLogging {
  def scalaListToGProMList(gpStructures: Seq[GProMStructure]) : GProMList.ByReference = {
    if(gpStructures.isEmpty)
      return null;
    val gpromExprs = new GProMList.ByReference()
    gpromExprs.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
    var j = 0;
    var exprListCell = gpromExprs.head
    gpStructures.foreach(gpromExpr => {
      val newArgCell = createGProMListCell(gpromExpr)
      if(j == 0){
        gpromExprs.head = newArgCell
        exprListCell = gpromExprs.head
      }
      else{
        exprListCell.next = newArgCell
        exprListCell = exprListCell.next
      }
      j+=1
      gpromExprs.length += 1
    })
    gpromExprs.write()
    gpromExprs
  }
  
  def scalaListToGProMListInt(ints: Seq[Int]) : GProMList.ByReference = {
    if(ints.isEmpty)
      return null;
    val gpromExprs = new GProMList.ByReference()
    gpromExprs.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
    var j = 0;
    var exprListCell = gpromExprs.head
    ints.foreach(intValue => {
      val newArgCell = createGProMListCell(intValue)
      if(j == 0){
        gpromExprs.head = newArgCell
        exprListCell = gpromExprs.head
      }
      else{
        exprListCell.next = newArgCell
        exprListCell = exprListCell.next
      }
      j+=1
      gpromExprs.length += 1
    })
    gpromExprs.write()
    gpromExprs
  }
  
  def gpromListToScalaList(list: GProMList) : List[GProMStructure] = {
    if(list == null)
      List[GProMStructure]()
    else{
      var scList = Seq[GProMStructure]()
      var listCell = list.head
      var i  = 1
      while(listCell != null){
        val projInput = GProMWrapper.inst.castGProMNode(new GProMNode(listCell.data.ptr_value))
        if(projInput == null){
          logger.error("WTF... there is some issue this should not be null")
          //TODO: not sure why, but sometimes one or two project args are null for det cols 
          //     -this is a temp hack until i figure it out 
          val intPtr = new com.sun.jna.Memory(com.sun.jna.Native.getNativeSize(classOf[Int]))
          intPtr.setInt(0, 1);
          scList = scList :+ new org.gprom.jdbc.jna.GProMConstant.ByValue(GProM_JNA.GProMNodeTag.GProM_T_Constant, GProM_JNA.GProMDataType.GProM_DT_INT,intPtr,0)
        }
        else{
          scList = scList :+ projInput
        }
        listCell = listCell.next
        i+=1
      }
      scList.toList
    }
  }
  
  def gpromIntPointerListToScalaList(list: GProMList) : List[Int] = {
    if(list == null)
      List[Int]()
    else{
      var scList = Seq[Int]()
      var listCell = list.head
      var i  = 1
      while(listCell != null){
        val projInput = listCell.data.int_value
        listCell = listCell.next
        scList =  scList :+ projInput 
        i+=1
      }
      scList.toList
    }
  }
  
   def createGProMQueryOperatorParentsList(parent:GProMStructure) : GProMList.ByReference = {
    val parentsList = new GProMList.ByReference()
    parentsList.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
    val listCell = new GProMListCell.ByReference()
    val dataUnion = new GProMListCell.data_union.ByValue(parent.getPointer())
    listCell.data = dataUnion
    listCell.next = null
    parentsList.head = listCell
    parentsList.length = 1
    parentsList.write()
    parentsList
  }
  
  def createGProMListCell(gpromDataNode:GProMStructure) : GProMListCell.ByReference = {
    val listCell = new GProMListCell.ByReference()
    val dataUnion = new GProMListCell.data_union.ByValue(gpromDataNode.getPointer())
    listCell.data = dataUnion
    listCell.next = null
    //listCell.write()
    listCell
  }
  
  def createGProMListCell(intValue:Int) : GProMListCell.ByReference = {
    val listCell = new GProMListCell.ByReference()
    val dataUnion = new GProMListCell.data_union.ByValue(intValue)
    listCell.data = dataUnion
    listCell.next = null
    //listCell.write()
    listCell
  }
  
  def getGProMDataTypeFromMimirType(mimirType : Type) : Int = {
    mimirType match {
        case TString() => GProM_JNA.GProMDataType.GProM_DT_STRING
        case TBool() => GProM_JNA.GProMDataType.GProM_DT_BOOL
        case TFloat() => GProM_JNA.GProMDataType.GProM_DT_FLOAT
        case TInt() => GProM_JNA.GProMDataType.GProM_DT_INT
        case _ => GProM_JNA.GProMDataType.GProM_DT_STRING
      }
  }
  
  def getMimirTypeFromGProMDataType(gpromType : Int) : Type = {
    gpromType match {
        case GProM_JNA.GProMDataType.GProM_DT_VARCHAR2 => new TString()
        case GProM_JNA.GProMDataType.GProM_DT_BOOL => new TBool()
        case GProM_JNA.GProMDataType.GProM_DT_FLOAT => new TFloat()
        case GProM_JNA.GProMDataType.GProM_DT_INT => new TInt()
        case GProM_JNA.GProMDataType.GProM_DT_LONG => new TInt()
        case GProM_JNA.GProMDataType.GProM_DT_STRING => new TString()
        case _ => new TAny()
      }
  }
  
  def translateMimirSchemaToGProMSchema(name:String, mimirSchema:Seq[(String, Type)]) : GProMSchema.ByReference = {
   val scmByRef = new GProMSchema.ByReference()
    scmByRef.`type` = GProM_JNA.GProMNodeTag.GProM_T_Schema
    scmByRef.name = name
    scmByRef.attrDefs = scalaListToGProMList(mimirSchema
        .map(sche => 
          new GProMAttributeDef.ByValue(GProM_JNA.GProMNodeTag.GProM_T_AttributeDef, 
              getGProMDataTypeFromMimirType(sche._2 ), sche._1)))
    //scmByRef.write()
    scmByRef
  }
  
  def translateGProMSchemaToMimirSchema(oper: GProMQueryOperatorNode): Seq[(String, Type)] = {
    oper.op.`type` match {
           case GProM_JNA.GProMNodeTag.GProM_T_AggregationOperator => {
             GProMWrapper.inst.castGProMNode(new GProMNode(oper.getPointer)) match {
               case aggregate : GProMAggregationOperator => {
                 if(aggregate.groupBy != null && aggregate.groupBy.length > 0){
                   val transSch = translateGProMSchemaToMimirSchema(oper.op.schema)
                   transSch.reverse.head +: transSch.reverse.tail.reverse 
                 }
                 else translateGProMSchemaToMimirSchema(oper.op.schema)
               }
             }
           }
           case _ => translateGProMSchemaToMimirSchema(oper.op.schema)
         }
  }
  
  private def translateGProMSchemaToMimirSchema(gpromSchema:GProMSchema.ByReference): Seq[(String, Type)] = {
    gpromListToScalaList(gpromSchema.attrDefs).map(attrDef =>  new GProMAttributeDef(attrDef.getPointer))
    .map(attrDef => (attrDef.attrName.replace("(", "_").replace(")",""), getMimirTypeFromGProMDataType(attrDef.dataType)) )
  }
}