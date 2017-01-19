package mimir.gprom.algebra

import org.gprom.jdbc.jna._

object OperatorTranslation {
  
  def gpromStructureToMimirOperator(topOperator: mimir.algebra.Operator, gpromStruct: GProMStructure ) : mimir.algebra.Operator = {
    var topOp = topOperator
    gpromStruct match {
      case aggregationOperator : GProMAggregationOperator => { 
        null
        }
      case attributeDef : GProMAttributeDef => { 
        null 
        }
      case constRelOperator : GProMConstRelOperator => { 
        null 
        }
      case constant : GProMConstant => { 
        null 
        }
      case duplicateRemoval : GProMDuplicateRemoval => { 
        null 
        }
      case joinOperator : GProMJoinOperator => { 
        null 
        }
      case list:GProMList => {
        val listHead = list.head
        gpromStructureToMimirOperator(topOperator, listHead)
      }
      case listCell : GProMListCell => { 
        val listCellDataGPStructure = new GProMNode(listCell.data.ptr_value)
        val cnvNode = GProMWrapper.inst.castGProMNode(listCellDataGPStructure);
        if(topOp == null){
          topOp = gpromStructureToMimirOperator(null, cnvNode)
          if(listCell.next != null)
            gpromStructureToMimirOperator(topOp, listCell.next)
          else
            topOp
        }
        else{
          cnvNode match { 
            case projectionOperator : GProMProjectionOperator => { 
              null 
              }
            case provenanceComputation : GProMProvenanceComputation => { 
              null 
              }
            case provenanceTransactionInfo : GProMProvenanceTransactionInfo => { 
              null 
              }
            case queryOperator : GProMQueryOperator => { 
              null 
              }
            case schema : GProMSchema => { 
              null 
              }
            case selectionOperator : GProMSelectionOperator => { 
              null 
              }
          }
        }
      }
      case nestingOperator : GProMNestingOperator => { 
        null 
        }
      case node : GProMNode => { 
        null 
        }
      case orderOperator : GProMOrderOperator => { 
        null 
        }
      case projectionOperator : GProMProjectionOperator => {
        val sourceChild = gpromStructureToMimirOperator(topOp, projectionOperator.op)
        val columns = getProjectionColumnsFromGProMProjectionOperator(projectionOperator)
        new mimir.algebra.Project(columns, sourceChild)
      }
      case provenanceComputation : GProMProvenanceComputation => { 
        null 
        }
      case provenanceTransactionInfo : GProMProvenanceTransactionInfo => { 
        null 
        }
      case queryOperator : GProMQueryOperator => { 
        queryOperator.`type` match {
          case GProM_JNA.GProMNodeTag.GProM_T_ProjectionOperator => gpromStructureToMimirOperator(topOp,queryOperator.inputs)
          case _ => null
        }
      }
      case schema : GProMSchema => { 
        null 
        }
      case selectionOperator : GProMSelectionOperator => { 
        null 
        }
      case setOperator : GProMSetOperator => { 
        null 
        }
      case tableAccessOperator : GProMTableAccessOperator => { 
        val tableSchema = getSchemaFromGProMTableAccessOperator(tableAccessOperator)
        val tableMeta = Seq[(String,mimir.algebra.Expression,mimir.algebra.Type)]() //tableSchema.map(tup => (tup._1,null,tup._2))
        new mimir.algebra.Table(tableAccessOperator.tableName, tableSchema, tableMeta)
      }
      case updateOperator : GProMUpdateOperator => { 
        null 
        }
      case _ => { 
        null 
        }
    }
    
  }
  
  
  def getProjectionColumnsFromGProMProjectionOperator(gpromProjOp : GProMProjectionOperator) : Seq[mimir.algebra.ProjectArg] = {
    val projExprs = gpromProjOp.projExprs;
    val projOpInputs =  gpromProjOp.op.inputs
    var listCell = projOpInputs.head
    var projTableName = ""
    for(i <- 1 to projOpInputs.length ){
      val projInput = GProMWrapper.inst.castGProMNode(new GProMNode(listCell.data.ptr_value))
      projTableName = projInput match {
        case tableAccessOperator : GProMTableAccessOperator => {
          tableAccessOperator.tableName + "_"
        }
        case _ => {
          ""
        }
      }
      listCell = listCell.next
    }
    
    listCell = projExprs.head
    val columns : Seq[mimir.algebra.ProjectArg] =
    for(i <- 1 to projExprs.length ) yield {
      val projExpr = GProMWrapper.inst.castGProMNode(new GProMNode(listCell.data.ptr_value))
      val projArg = projExpr match {
        case attrRef : GProMAttributeReference => {
          new mimir.algebra.ProjectArg(attrRef.name, new mimir.algebra.Var(projTableName + attrRef.name))
        }
        case _ => null
      }
      listCell = listCell.next
      projArg
    }
    columns
  }
  
  def getSchemaFromGProMTableAccessOperator(gpromTableOp : GProMTableAccessOperator) : Seq[(String, mimir.algebra.Type)] = {
    val attrDefList = gpromTableOp.op.schema.attrDefs;
    var listCell = attrDefList.head
    val columns : Seq[(String, mimir.algebra.Type)] =
    for(i <- 1 to attrDefList.length ) yield {
      val attrDef = new GProMAttributeDef(listCell.data.ptr_value)
      val attrType = attrDef.dataType match {
        case GProM_JNA.GProMDataType.GProM_DT_VARCHAR2 => new mimir.algebra.TString()
        case GProM_JNA.GProMDataType.GProM_DT_BOOL => new mimir.algebra.TBool()
        case GProM_JNA.GProMDataType.GProM_DT_FLOAT => new mimir.algebra.TFloat()
        case GProM_JNA.GProMDataType.GProM_DT_INT => new mimir.algebra.TInt()
        case GProM_JNA.GProMDataType.GProM_DT_LONG => new mimir.algebra.TInt()
        case GProM_JNA.GProMDataType.GProM_DT_STRING => new mimir.algebra.TString()
        case _ => new mimir.algebra.TAny()
      }
      val schItem = (gpromTableOp.tableName + "_" +attrDef.attrName, attrType)
      listCell = listCell.next
      schItem
    }
    columns
  }
  
}