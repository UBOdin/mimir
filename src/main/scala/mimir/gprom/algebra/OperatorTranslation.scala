package mimir.gprom.algebra

import org.gprom.jdbc.jna._
import com.sun.jna.Pointer
import com.sun.jna.Memory
import com.sun.jna.Native
import mimir.algebra._

object ProjectionArgVisibility extends Enumeration {
   val Visible = Value("Visible")
   val Invisible = Value("Invisible") 
} 

object OperatorTranslation {
  
  def gpromStructureToMimirOperator(depth : Int, gpromStruct: GProMStructure, gpromParentStruct: GProMStructure ) : Operator = {
    gpromStruct match {
      case aggregationOperator : GProMAggregationOperator => { 
        throw new Exception("Translation Not Yet Implemented '"+aggregationOperator+"'")
        }
      case attributeDef : GProMAttributeDef => { 
        throw new Exception("Translation Not Yet Implemented '"+attributeDef+"'") 
        }
      case constRelOperator : GProMConstRelOperator => { 
        throw new Exception("Translation Not Yet Implemented '"+constRelOperator+"'") 
        }
      case constant : GProMConstant => { 
        throw new Exception("Translation Not Yet Implemented '"+constant+"'") 
        }
      case duplicateRemoval : GProMDuplicateRemoval => { 
        throw new Exception("Translation Not Yet Implemented '"+duplicateRemoval+"'") 
        }
      case joinOperator : GProMJoinOperator => { 
        throw new Exception("Translation Not Yet Implemented '"+joinOperator+"'") 
        }
      case list:GProMList => {
        val listHead = list.head
        gpromStructureToMimirOperator(depth, listHead, gpromParentStruct)
      }
      case listCell : GProMListCell => { 
        val listCellDataGPStructure = new GProMNode(listCell.data.ptr_value)
        val cnvNode = GProMWrapper.inst.castGProMNode(listCellDataGPStructure);
        val retOp = gpromStructureToMimirOperator(depth, cnvNode, gpromParentStruct)
        if(listCell.next != null)
           throw new Exception("There are more operators in the list but they are not being translated '"+listCell.next+"'") 
        retOp
      }
      case nestingOperator : GProMNestingOperator => { 
        throw new Exception("Translation Not Yet Implemented '"+nestingOperator+"'") 
        }
      case node : GProMNode => { 
        throw new Exception("Translation Not Yet Implemented '"+node+"'") 
        }
      case orderOperator : GProMOrderOperator => { 
        throw new Exception("Translation Not Yet Implemented '"+orderOperator+"'") 
        }
      case projectionOperator : GProMProjectionOperator => {
        val sourceChild = gpromStructureToMimirOperator(depth+1, projectionOperator.op, projectionOperator)
        val projArgs = getProjectionColumnsFromGProMProjectionOperator(projectionOperator)
        val visibleProjArgs = projArgs.map{ projArgT => projArgT._2 match { case ProjectionArgVisibility.Visible => Some(projArgT._1); case _ => None }}.flatten
        val invisibleProjArgs = projArgs.map{ projArgT => projArgT._2 match { case ProjectionArgVisibility.Invisible => Some(projArgT._1); case _ => None }}.flatten
        val invisibleSchema = projArgs.map{ projArgT => projArgT._2 match { case ProjectionArgVisibility.Invisible => Some((projArgT._1, projArgT._3, projArgT._4)); case _ => None }}.flatten
         
        if(projectionOperator.op.provAttrs != null && projectionOperator.op.provAttrs.length > 0 && depth == 0){
          new Recover(new Project(visibleProjArgs, sourceChild), invisibleSchema)
        }
        else if(projectionOperator.op.provAttrs != null && projectionOperator.op.provAttrs.length > 0 ){
          new Project(visibleProjArgs, new Annotate(sourceChild, invisibleSchema))
        }
        else
          new Project(visibleProjArgs, sourceChild)
      }
      case provenanceComputation : GProMProvenanceComputation => { 
        throw new Exception("Translation Not Yet Implemented '"+provenanceComputation+"'") 
        }
      case provenanceTransactionInfo : GProMProvenanceTransactionInfo => { 
        throw new Exception("Translation Not Yet Implemented '"+provenanceTransactionInfo+"'") 
        }
      case queryOperator : GProMQueryOperator => { 
        queryOperator.`type` match {
          case GProM_JNA.GProMNodeTag.GProM_T_ProjectionOperator => gpromStructureToMimirOperator(depth+1, queryOperator.inputs, queryOperator)
          case GProM_JNA.GProMNodeTag.GProM_T_SelectionOperator => gpromStructureToMimirOperator(depth+1, queryOperator.inputs, queryOperator)
          case _ => gpromStructureToMimirOperator(depth+1, queryOperator.inputs, queryOperator)
        }
      }
      case schema : GProMSchema => { 
        throw new Exception("Translation Not Yet Implemented '"+schema+"'") 
        }
      case selectionOperator : GProMSelectionOperator => { 
          val tableIntermSch = extractTableSchemaGProMOperator(selectionOperator)
          val condition = translateGProMConditionToMimirExpression(new GProMNode(selectionOperator.cond.getPointer), tableIntermSch) 
          val sourceChild = gpromStructureToMimirOperator(depth+1, selectionOperator.op, selectionOperator)
          new Select(condition, sourceChild)
        }
      case setOperator : GProMSetOperator => { 
        throw new Exception("Translation Not Yet Implemented '"+setOperator+"'") 
        }
      case tableAccessOperator : GProMTableAccessOperator => { 
          val tableIntermSchema = extractTableSchemaGProMOperator(tableAccessOperator)
          val tableSchema = tableIntermSchema.map(tis => (tis.getAttrPrefix()+tis.attrName, tis.attrType))//getSchemaFromGProMQueryOperator(tableIntermSchema, tableAccessOperator.op)
          val tableMeta = Seq[(String,Expression,Type)]() //tableSchema.map(tup => (tup._1,null,tup._2))
          new Table(tableAccessOperator.tableName, tableIntermSchema(0).alias, tableSchema, tableMeta)
      }
      case updateOperator : GProMUpdateOperator => { 
        throw new Exception("Translation Not Yet Implemented '"+updateOperator+"'") 
        }
      case _ => { 
        null 
        }
    }
    
  }
  
  def translateGProMConditionToMimirExpression(gpromCond : GProMNode, intermSchema : Seq[MimirToGProMIntermediateSchemaInfo]) : Expression = {
     val conditionNode = GProMWrapper.inst.castGProMNode(gpromCond)
     conditionNode match {
       case operator : GProMOperator => {
         val expressions = gpromListToScalaList(operator.args).toArray
         operator.name match {
            case "+" => new Arithmetic( Arith.Add, translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "-" => new Arithmetic( Arith.Sub, translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "*" => new Arithmetic( Arith.Mult, translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "/" => new Arithmetic( Arith.Div, translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "&" => new Arithmetic( Arith.And, translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "|" => new Arithmetic( Arith.Or, translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "AND" => new Arithmetic( Arith.And, translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "OR" => new Arithmetic( Arith.Or, translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "=" => new Comparison( Cmp.Eq , translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "<>" => new Comparison( Cmp.Neq, translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case ">" => new Comparison( Cmp.Gt, translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema)) 
            case ">=" => new Comparison( Cmp.Gte , translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "<" => new Comparison( Cmp.Lt , translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "<=" => new Comparison( Cmp.Lte , translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "LIKE" => new Comparison( Cmp.Like , translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "NOT LIKE" => new Comparison( Cmp.NotLike, translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case x => translateGProMStructureToMimirExpression(expressions(0), intermSchema);
        }    
       }
       case x => translateGProMStructureToMimirExpression(conditionNode, intermSchema);
     }
  }
  
  def translateGProMStructureToMimirExpression(gpromStruct : GProMStructure, intermSchema : Seq[MimirToGProMIntermediateSchemaInfo]) : Expression = {
    gpromStruct match {
      case operator : GProMOperator => {
        translateGProMConditionToMimirExpression(new GProMNode(operator.getPointer()), intermSchema)
      }
      case attributeReference : GProMAttributeReference => {
        val attrPrefix = intermSchema.find(ise => ise.attrName.equals(attributeReference.name)).get.getAttrPrefix
        new Var(attrPrefix+attributeReference.name)
      }
      case constant : GProMConstant => {
      	if(constant.isNull == 1)
      	 new NullPrimitive()
      	else
      	  constant.constType match {
        	  case GProM_JNA.GProMDataType.GProM_DT_VARCHAR2 => new StringPrimitive(constant.value.getString(0))
            case GProM_JNA.GProMDataType.GProM_DT_BOOL => new BoolPrimitive(constant.value.getInt(0)==1)
            case GProM_JNA.GProMDataType.GProM_DT_FLOAT => new FloatPrimitive(constant.value.getFloat(0))
            case GProM_JNA.GProMDataType.GProM_DT_INT => new IntPrimitive(constant.value.getInt(0))
            case GProM_JNA.GProMDataType.GProM_DT_LONG => new IntPrimitive(constant.value.getLong(0))
            case GProM_JNA.GProMDataType.GProM_DT_STRING => new StringPrimitive(constant.value.getString(0))
            case _ => new NullPrimitive()
      	}
      }
      case caseExpr : GProMCaseExpr => {
      	throw new Exception("Expression Translation Not Yet Implemented '"+caseExpr+"'")
      }
      case caseWhen : GProMCaseWhen => {
      	throw new Exception("Expression Translation Not Yet Implemented '"+caseWhen+"'")
      }
      case castExpr : GProMCastExpr => {
      	throw new Exception("Expression Translation Not Yet Implemented '"+castExpr+"'")
      }
      case functionCall : GProMFunctionCall => {
      	throw new Exception("Expression Translation Not Yet Implemented '"+functionCall+"'")
      }
      case isNullExpr : GProMIsNullExpr => {
      	throw new Exception("Expression Translation Not Yet Implemented '"+isNullExpr+"'")
      }
      case orderExpr : GProMOrderExpr => {
      	throw new Exception("Expression Translation Not Yet Implemented '"+orderExpr+"'")
      }
      case rowNumExpr : GProMRowNumExpr => {
      	throw new Exception("Expression Translation Not Yet Implemented '"+rowNumExpr+"'")
      }
      case sQLParameter : GProMSQLParameter => {
      	throw new Exception("Expression Translation Not Yet Implemented '"+sQLParameter+"'")
      }
      case windowBound : GProMWindowBound => {
      	throw new Exception("Expression Translation Not Yet Implemented '"+windowBound+"'")
      }
      case windowDef : GProMWindowDef => {
      	throw new Exception("Expression Translation Not Yet Implemented '"+windowDef+"'")
      }
      case windowFrame : GProMWindowFrame => {
      	throw new Exception("Expression Translation Not Yet Implemented '"+windowFrame+"'")
      }
      case windowFunction : GProMWindowFunction => {
      	throw new Exception("Expression Translation Not Yet Implemented '"+windowFunction+"'")
      }
      case x => {
        throw new Exception("Expression Translation Not Yet Implemented '"+x+"'")
      }
    }
  }
  
  /*def getInvisibleProvenanceSchemaFromGProMQueryOperator(queryOperator : GProMQueryOperator) : Seq[(String, Type)] = {
    val provAttrs = gpromIntPointerListToScalaList(queryOperator.provAttrs)
    val tableIntermSchema = extractTableSchemaGProMOperator(queryOperator)
    tableIntermSchema.zipWithIndex.map{ case (attr, index) => {
          if(provAttrs.contains(index))
            Some(attr)
          else
            None
        }
      }.flatten
  }*/
  
  def gpromListToScalaList(list: GProMList) : List[GProMStructure] = {
    if(list == null)
      List[GProMStructure]()
    else{
      var listCell = list.head
      (for(i <- 1 to list.length ) yield {
        val projInput = GProMWrapper.inst.castGProMNode(new GProMNode(listCell.data.ptr_value))
        listCell = listCell.next
        projInput
      }).toList
    }
  }
  
  def gpromIntPointerListToScalaList(list: GProMList) : List[Int] = {
    if(list == null)
      List[Int]()
    else{
      var listCell = list.head
      (for(i <- 1 to list.length ) yield {
        val projInput = listCell.data.int_value
        listCell = listCell.next
        projInput
      }).toList
    }
  }
  
trait Enum[A] {
  trait Value { self: A =>
    _values :+= this
  }
  private var _values = List.empty[A]
  def values = _values
}

sealed trait ProjectionArgVisibility extends ProjectionArgVisibility.Value
object ProjectionArgVisibility extends Enum[ProjectionArgVisibility] {
  case object Visible extends ProjectionArgVisibility;  Visible 
  case object Invisible extends ProjectionArgVisibility; Invisible 
}
  
  def getProjectionColumnsFromGProMProjectionOperator(gpromProjOp : GProMProjectionOperator) : Seq[(ProjectArg, ProjectionArgVisibility.Value, (String, Type), String)] = {
    val projExprs = gpromProjOp.projExprs;
    val projOpInputs =  gpromProjOp.op.inputs
    val tableSchema = extractTableSchemaGProMOperator(projOpInputs)
    val projSchema = getIntermediateSchemaFromGProMSchema(gpromProjOp.op.schema)
    
   val provAttrs = gpromIntPointerListToScalaList(gpromProjOp.op.provAttrs)
      
    var listCell = projExprs.head
    for(i <- 1 to projExprs.length ) yield {
      val scmPrefix = tableSchema(i-1).getAttrPrefix()
      val projExpr = new GProMNode(listCell.data.ptr_value)
      val mimirExpr = translateGProMConditionToMimirExpression(projExpr, tableSchema )
      val projVisibility = {
        if(!provAttrs.contains(i-1))
          ProjectionArgVisibility.Visible
        else
          ProjectionArgVisibility.Invisible 
      }
      listCell = listCell.next
      (new ProjectArg(projSchema(i-1).attrName, mimirExpr), projVisibility, (projSchema(i-1).attrName, projSchema(i-1).attrType), tableSchema(i-1).alias)
    }
  }
  
  def extractTableSchemaGProMOperator(gpromOper : GProMStructure) : Seq[MimirToGProMIntermediateSchemaInfo] = {
    gpromOper match {
        case tableAccessOperator : GProMTableAccessOperator => {
          val schemaName = tableAccessOperator.op.schema.name
          val schemaNames = schemaName match { 
              case "" => {
                tableAccessOperator.tableName match { 
                  case "" => ("", "", "")
                  case _ => (tableAccessOperator.tableName, tableAccessOperator.tableName, tableAccessOperator.tableName + "_")
                }  
              }
              case _ => (tableAccessOperator.tableName, schemaName, schemaName + "_") 
          }
          var listCell = tableAccessOperator.op.schema.attrDefs.head
          (for(i <- 1 to tableAccessOperator.op.schema.attrDefs.length ) yield {
            val attrDef = new GProMAttributeDef(listCell.data.ptr_value)
            listCell = listCell.next
            new MimirToGProMIntermediateSchemaInfo(schemaNames._1, schemaNames._2, attrDef.attrName, schemaNames._2 + attrDef.attrName, "", getMimirTypeFromGProMDataType(attrDef.dataType), i-1)
          }).toSeq
        }
        case projectionOperator : GProMProjectionOperator => { 
            extractTableSchemaGProMOperator(projectionOperator.op.inputs)
          }
        case provenanceComputation : GProMProvenanceComputation => { 
          throw new Exception("Attribute Prefix Extraction  Not Yet Implemented '"+provenanceComputation+"'") 
          }
        case provenanceTransactionInfo : GProMProvenanceTransactionInfo => { 
          throw new Exception("Attribute Prefix Extraction  Not Yet Implemented '"+provenanceTransactionInfo+"'") 
          }
        /*case queryOperator : GProMQueryOperator => { 
            queryOperator.inputs match {
              case null =>{
                val schemaName = queryOperator.schema.name
                val schemaNames = schemaName match { 
                    case "" => ("", "", "")
                    case _ => (schemaName, schemaName, schemaName + "_") 
                }
                var listCell = queryOperator.schema.attrDefs.head
                (for(i <- 1 to queryOperator.schema.attrDefs.length ) yield {
                  val attrDef = new GProMAttributeDef(listCell.data.ptr_value)
                  listCell = listCell.next
                  new MimirToGProMIntermediateSchemaInfo(schemaNames._1, schemaNames._2, attrDef.attrName, schemaNames._2 + attrDef.attrName, "", getMimirTypeFromGProMDataType(attrDef.dataType), i-1)
                }).toSeq
              }
              case x => extractTableSchemaGProMOperator(queryOperator.inputs)
            }
          }
        case schema : GProMSchema => { 
            val schemaNames = schema.name match { 
                case "" => ("", "", "")
                case _ => (schema.name, schema.name, schema.name + "_") 
            }
            var listCell = schema.attrDefs.head
            (for(i <- 1 to schema.attrDefs.length ) yield {
              val attrDef = new GProMAttributeDef(listCell.data.ptr_value)
              listCell = listCell.next
              new MimirToGProMIntermediateSchemaInfo(schemaNames._1, schemaNames._2, attrDef.attrName, schemaNames._2 + attrDef.attrName, "", getMimirTypeFromGProMDataType(attrDef.dataType), i-1)
            }).toSeq
          }*/
        case selectionOperator : GProMSelectionOperator => { 
            extractTableSchemaGProMOperator(selectionOperator.op.inputs)
          }
        case list:GProMList => {
            var listCell = list.head
            var reurnSeq = Seq[MimirToGProMIntermediateSchemaInfo]()
            for(i <- 1 to list.length ) {
              reurnSeq = reurnSeq.union(extractTableSchemaGProMOperator( listCell))
              listCell = listCell.next
            }
            reurnSeq
          }
          case listCell : GProMListCell => { 
            val listCellDataGPStructure = new GProMNode(listCell.data.ptr_value)
            val cnvNode = GProMWrapper.inst.castGProMNode(listCellDataGPStructure);
            val retOp = extractTableSchemaGProMOperator(cnvNode)
            if(listCell.next != null)
               throw new Exception("There are more operators in the list but they are not being translated '"+listCell.next+"'") 
            retOp
          }
        case x => {
          throw new Exception("Attribute Prefix Extraction  Not Yet Implemented '"+x+"'") 
        }
      }
  }
  
  def getIntermediateSchemaFromGProMSchema(gpromSchema: GProMSchema) : Seq[MimirToGProMIntermediateSchemaInfo] = {
    val schemaNames = gpromSchema.name match { 
        case "" => ("", "", "")
        case _ => (gpromSchema.name, gpromSchema.name, gpromSchema.name + "_") 
    }
    var listCell = gpromSchema.attrDefs.head
    (for(i <- 1 to gpromSchema.attrDefs.length ) yield {
      val attrDef = new GProMAttributeDef(listCell.data.ptr_value)
      listCell = listCell.next
      new MimirToGProMIntermediateSchemaInfo(schemaNames._1, schemaNames._2, attrDef.attrName, schemaNames._2 + attrDef.attrName, "", getMimirTypeFromGProMDataType(attrDef.dataType), i-1)
    }).toSeq
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
  
  /*def getSchemaFromGProMQueryOperator(schema: Seq[MimirToGProMIntermediateSchemaInfo], gpromQueryOp : GProMQueryOperator) : Seq[(String, Type)] = {
    val attrDefList = gpromQueryOp.schema.attrDefs;
    val attrPrefixIdxLookup = schema.map(se => se.attrName)
    
    var listCell = attrDefList.head
    val columns : Seq[(String, Type)] =
    for(i <- 1 to attrDefList.length ) yield {
      val attrDef = new GProMAttributeDef(listCell.data.ptr_value)
      val attrType = getMimirTypeFromGProMDataType(attrDef.dataType)
      val schRefIdx = attrPrefixIdxLookup.indexOf(attrDef.attrName)
      val attrPrefix = schema(schRefIdx).getAttrPrefix()
      val tableAndAttrName = attrPrefix +attrDef.attrName
      val schItem = ( tableAndAttrName, attrType)
      listCell = listCell.next
      schItem
    }
    columns
  }
  
  def getSchemaWithProvFromGProMQueryOperator(schema : Seq[MimirToGProMIntermediateSchemaInfo], gpromQueryOp : GProMQueryOperator) : (Seq[(String, Type)], List[Int]) = {
    val provAttrs = gpromIntPointerListToScalaList(gpromQueryOp.provAttrs)
    
    val attrDefList = gpromQueryOp.schema.attrDefs
    val attrPrefixIdxLookup = schema.map(se => se.attrName)
    var listCell = attrDefList.head
    val columns : Seq[(String, Type)] =
    for(i <- 1 to attrDefList.length ) yield {
      val attrDef = new GProMAttributeDef(listCell.data.ptr_value)
      val attrType = getMimirTypeFromGProMDataType(attrDef.dataType)
      val schRefIdx = attrPrefixIdxLookup.indexOf(attrDef.attrName)
      val attrPrefix = schema(schRefIdx).getAttrPrefix()
      var schItem : (String, Type) = null;
      if(!provAttrs.contains(i-1)){
        val tableAndAttrName = attrPrefix +attrDef.attrName
        schItem = (tableAndAttrName, attrType)
      }
      else
        schItem = ( attrDef.attrName, attrType)
      listCell = listCell.next
      schItem
    }
    (columns, provAttrs)
  }*/
  
  class MimirToGProMIntermediateSchemaInfo(val name : String, val alias : String, val attrName : String, val attrMimirName : String, val attrProjectedName : String, val attrType : Type, val attrPosition : Int)  {
    def getAttrPrefix() : String = {
      alias match { 
          case "" => ""
          case x =>  x + "_"
      }
    }
  }
 /*( def mimirOperatorToGProMList(mimirOperator :  Operator) : GProMList.ByReference = {
    mimirOperatorToGProMList(mimirOperator, extractTableSchemaForGProM(mimirOperator))
  }*/
  
  def mimirOperatorToGProMList(mimirOperator :  Operator) : GProMList.ByReference = {
    val list = new GProMList.ByReference()
    list.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
    list.length = 0;
    
    mimirOperator match {
			case Project(cols, src) => {
			 val schTable = extractTableSchemaForGProM(mimirOperator)
       val schProj = getSchemaForGProM(mimirOperator)
			 val toQoScm = translateMimirSchemaToGProMSchema("PROJECTION", schProj)
			  val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_ProjectionOperator,  mimirOperatorToGProMList(src), toQoScm, null, null, null)
			  list.head = createGProMListCell(new GProMProjectionOperator.ByValue(gqo, translateMimirProjArgsToGProMList(schTable, cols))) 
			   list.length += 1; 
			  list
			 }
			case ProvenanceOf(psel) => {
			  list
			}
			case Annotate(subj,invisScm) => {
        list
      }
			case Recover(subj,invisScm) => {
        list
      }
			case Select(cond, src) => {
			  val schTable = getSchemaForGProM(mimirOperator)
        val toQoScm = translateMimirSchemaToGProMSchema("SELECT", schTable)
        val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_SelectionOperator,  mimirOperatorToGProMList(src), toQoScm, null, null, null) 
        val gpcond = translateMimirExpressionToGProMCondition(cond, schTable) 
        val gpnbr = new GProMNode.ByReference(gpcond.getPointer)
			  val gpselop = new GProMSelectionOperator.ByValue(gqo, gpnbr )
			  list.head = createGProMListCell(gpselop) 
			  list.length += 1; 
			  list
			}
			case Aggregate(groupBy, agggregates, source) => {
			  list
			}

			case Join(lhs, rhs) => { 
			  list
			}
			case LeftOuterJoin(lhs, rhs, condition) => {
			  list
			}
			case Table(name, alias, sch, meta) => {
			  val schTable = getSchemaForGProM(mimirOperator)
			  val toQoScm = translateMimirSchemaToGProMSchema(alias, schTable)//mimirOperator)
			  val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_TableAccessOperator, null, toQoScm, null, null, null)
			  val gpromTable = new GProMTableAccessOperator.ByValue(gqo,name,null)
			  list.head = createGProMListCell(gpromTable)
			  list.length += 1 
			  list
			}

		}
  }
  
  def translateMimirExpressionToGProMCondition(mimirExpr : Expression, schema : Seq[MimirToGProMIntermediateSchemaInfo]) : GProMStructure = {
     
         mimirExpr match {
            case Comparison(op, lhs, rhs) => {
              val cmpOp = op match {
                case  Cmp.Eq => "=" 
                case  Cmp.Neq  => "<>" 
                case  Cmp.Gt  => ">" 
                case  Cmp.Gte  => ">=" 
                case  Cmp.Lt  => "<" 
                case  Cmp.Lte  => "<=" 
                case  Cmp.Like  => "LIKE" 
                case  Cmp.NotLike => "NOT LIKE" 
                case x => throw new Exception("Invalid operand '"+x+"'")
              }
              val list = new GProMList.ByReference()
               list.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
               list.head = createGProMListCell(translateMimirExpressionToGProMStructure(lhs, schema)) 
			         list.head.next = createGProMListCell(translateMimirExpressionToGProMStructure(rhs, schema))      
               list.length = 2;
               new GProMOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_Operator, cmpOp,list)
            }
            case Arithmetic(op, lhs, rhs) => {
              val aritOp = op match {
                case  Arith.Add => "+" 
                case  Arith.Sub => "-" 
                case  Arith.Mult => "*" 
                case  Arith.Div => "/" 
                //case  Arith.And => "&" 
                //case  Arith.Or => "|" 
                case  Arith.And => "AND" 
                case  Arith.Or => "OR" 
                case x => throw new Exception("Invalid operand '"+x+"'")
              }
              val list = new GProMList.ByReference()
               list.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
               list.head = createGProMListCell(translateMimirExpressionToGProMStructure(lhs, schema)) 
			         list.head.next = createGProMListCell(translateMimirExpressionToGProMStructure(rhs, schema))      
               list.length = 2;
               new GProMOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_Operator, aritOp,list)
            }
            case x => {
              translateMimirExpressionToGProMStructure(x, schema)
            }
          }
  }
  
  def translateMimirExpressionToGProMStructure(mimirExpr : Expression, schema : Seq[MimirToGProMIntermediateSchemaInfo]) : GProMStructure = {
    mimirExpr match {
      case primitive : PrimitiveValue => translateMimirPrimitiveExpressionToGProMConstant(primitive)
      case Comparison(_,_,_) => translateMimirExpressionToGProMCondition(mimirExpr, schema)
      case Arithmetic(_,_,_) => translateMimirExpressionToGProMCondition(mimirExpr, schema)
      case Conditional(_,_,_) => translateMimirExpressionToGProMCondition(mimirExpr, schema)
      case Var(v) => {
        val indexOfCol = schema.map(ct => ct.attrMimirName).indexOf(v)
        val attrRef = new GProMAttributeReference.ByValue(GProM_JNA.GProMNodeTag.GProM_T_AttributeReference, schema(indexOfCol).attrName, 0, schema(indexOfCol).attrPosition, 0, getGProMDataTypeFromMimirType(schema(indexOfCol).attrType ))
        attrRef
      }
      case x => {
        throw new Exception("Expression Translation not implemented '"+x+"'")
      }
    }
      /*//GProMOperator 
      //GProMAttributeReference 
      //GProMConstant 
      GProMCaseExpr 
      GProMCaseWhen 
      GProMCastExpr
      GProMFunctionCall 
      GProMIsNullExpr 
      GProMOrderExpr
      GProMRowNumExpr
      GProMSQLParameter
      GProMWindowBound
      GProMWindowDef
      GProMWindowFrame
      GProMWindowFunction */
  }
  
  def translateMimirPrimitiveExpressionToGProMConstant(mimirPrimitive : PrimitiveValue) : GProMConstant.ByValue = {
    val typeValueIsNull = mimirPrimitive match {
      case IntPrimitive(v) => {
        val intPtr = new Memory(Native.getNativeSize(classOf[Int])).getPointer(0)
        intPtr.setInt(0, v.asInstanceOf[Int]);
        (GProM_JNA.GProMDataType.GProM_DT_INT,intPtr.getPointer(0),0)
      }
      case StringPrimitive(v) => {
        val strPtr = new Memory(v.length()+1)
        strPtr.setString(0, v);
        (GProM_JNA.GProMDataType.GProM_DT_STRING,strPtr,0)
      }
      case FloatPrimitive(v) => {
        val fltPtr = new Memory(Native.getNativeSize(classOf[Float])).getPointer(0)
        fltPtr.setFloat(0, v.asInstanceOf[Float]);
        (GProM_JNA.GProMDataType.GProM_DT_FLOAT,fltPtr,0)
      }
      case RowIdPrimitive(v) => {
        throw new Exception("Primitive Expression Translation not implemented '"+v+"'")
      }
      case BoolPrimitive(v) => {
        val intPtr = new Memory(Native.getNativeSize(classOf[Int])).getPointer(0)
        intPtr.setInt(0, v.asInstanceOf[Int]);
        (GProM_JNA.GProMDataType.GProM_DT_BOOL,intPtr,0)
      }
      case NullPrimitive() => {
        val intPtr = new Memory(Native.getNativeSize(classOf[Int])).getPointer(0)
        intPtr.setInt(0,0);
        (GProM_JNA.GProMDataType.GProM_DT_INT,intPtr,1)
      }
      case DatePrimitive(y,m,d) => {
        val dtStr = ""+y+"-"+m+"-"+d
        val strPtr = new Memory(dtStr.length()+1).getPointer(0)
        strPtr.setString(0, dtStr);
        (GProM_JNA.GProMDataType.GProM_DT_STRING,strPtr,0)
      }
    }
    val gpc = new GProMConstant.ByValue(GProM_JNA.GProMNodeTag.GProM_T_Constant, typeValueIsNull._1, typeValueIsNull._2, typeValueIsNull._3)
    gpc
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
  
  def getSchemaForGProM(oper : Operator) : Seq[MimirToGProMIntermediateSchemaInfo] = {
    oper match {
      case Project(args, child) => {
        val tableSchema = extractTableSchemaForGProM(child)
        val pargsOut = args.map(pa => pa.name)
        val pargsIn = args.map(pa => pa.expression)
        val plainSch = tableSchema.filter(se => pargsIn.indexOf(se.attrMimirName) >= 0).map(fse => new MimirToGProMIntermediateSchemaInfo(fse.name,fse.alias,fse.attrName,fse.attrMimirName,pargsOut(pargsIn.indexOf(fse.attrMimirName)),fse.attrType, fse.attrPosition))
        if(plainSch.length  != oper.schema.length){
          //TODO: replace this hack to get gprom expression attr names with something better
          val gpromExprAttrNames = args.map(pa => {
            val gpromProjOper = translateMimirExpressionToGProMCondition(pa.expression, tableSchema)
            translateGProMConditionToMimirExpression(new GProMNode(gpromProjOper.getPointer), tableSchema)
          })
          oper.schema.zipWithIndex.map(sch => {
            val exprNoAliasPattern = "(EXPR(?:_[0-9]+)?)".r
            val attrProjName = pargsOut(sch._2) match {
              case exprNoAliasPattern(mimirExprName) => (gpromExprAttrNames(sch._2).toString.trim, mimirExprName,"")
              case x => (gpromExprAttrNames(sch._2).toString.trim, pargsIn(sch._2).toString, x)
            }
            new MimirToGProMIntermediateSchemaInfo(tableSchema(0).name, tableSchema(0).alias, attrProjName._1, attrProjName._2, attrProjName._3, sch._1._2, sch._2)
          }).union(plainSch).sortBy(f => f.attrPosition)
        }
        else
          plainSch
      }
      case Select(cond, source) => {
        getSchemaForGProM(source)
      }
      case Join(lhs, rhs) => {
        getSchemaForGProM(lhs).union(getSchemaForGProM(rhs))
      }
      case LeftOuterJoin(lhs, rhs, cond) => {
        getSchemaForGProM(lhs).union(getSchemaForGProM(rhs))
      }
      case Table(name, alias, tgtSch, metadata) => {
         oper.schema.zipWithIndex.map(sch => new MimirToGProMIntermediateSchemaInfo(name, alias, sch._1._1.replaceFirst((alias + "_"), ""), sch._1._1, "", sch._1._2, sch._2))
      }
      case x => {
        throw new Exception("Can Not extract schema '"+x+"'")
      }
    }
  }
  
  
  def extractTableSchemaForGProM(oper : Operator ) : Seq[MimirToGProMIntermediateSchemaInfo] = {
    oper match {
      case Project(args, child) => {
        extractTableSchemaForGProM(child)
      }
      case Select(cond, source) => {
        extractTableSchemaForGProM(source)
      }
      case Join(lhs, rhs) => {
        extractTableSchemaForGProM(lhs).union(extractTableSchemaForGProM(rhs))
      }
      case LeftOuterJoin(lhs, rhs, cond) => {
        extractTableSchemaForGProM(lhs).union(extractTableSchemaForGProM(rhs))
      }
      case Table(name, alias, tgtSch, metadata) => {
        oper.schema.zipWithIndex.map(sch => new MimirToGProMIntermediateSchemaInfo(name, alias, sch._1._1.replaceFirst((alias + "_"), ""), sch._1._1, "", sch._1._2, sch._2))
      }
      case x => {
        throw new Exception("Can Not extract schema '"+x+"'")
      }
    }
  }
  
  
  def translateMimirSchemaToGProMSchema(schemaName: String, oper : Operator) : GProMSchema.ByReference = {
    translateMimirSchemaToGProMSchema(schemaName, extractTableSchemaForGProM(oper)) 
  }
  
  def translateMimirSchemaToGProMSchema(schemaName: String, schema : Seq[MimirToGProMIntermediateSchemaInfo]) : GProMSchema.ByReference = {
    var listCell : GProMListCell.ByReference = null
    var lastListCell : GProMListCell.ByReference = null
    var listTail : GProMListCell.ByReference = null
    var i = 0;
    
    
    for(schemaTup : MimirToGProMIntermediateSchemaInfo <- schema.reverse){
      val attrName = schemaTup.attrProjectedName match { 
              case "" => schemaTup.attrName
              case x => x
          } 
      val attrDef = new GProMAttributeDef.ByValue(GProM_JNA.GProMNodeTag.GProM_T_AttributeDef, getGProMDataTypeFromMimirType(schemaTup.attrType ), attrName);
      val dataUnion = new GProMListCell.data_union.ByValue(attrDef.getPointer)
      listCell = new GProMListCell.ByReference()
      if(i==0)
        listTail
      listCell.data = dataUnion
      listCell.next = lastListCell
      lastListCell = listCell;
      i+=1
    }
    val attrDefList = new GProMList.ByReference();
    attrDefList.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
    attrDefList.length = schema.length
    attrDefList.head = listCell
    attrDefList.tail = listTail
    val scmByRef = new GProMSchema.ByReference()
    scmByRef.`type` = GProM_JNA.GProMNodeTag.GProM_T_Schema
    scmByRef.name = schemaName
    scmByRef.attrDefs = attrDefList
    scmByRef
  }
  
  def translateMimirProjArgsToGProMList(schema : Seq[MimirToGProMIntermediateSchemaInfo], cols : Seq[ProjectArg]) : GProMList.ByReference = {
    var listCell : GProMListCell.ByReference = null
    var lastListCell : GProMListCell.ByReference = null
    var listTail : GProMListCell.ByReference = null
    var i = 0;
    for(projArg : ProjectArg <- cols.reverse){
      val projGpromOperator = translateMimirExpressionToGProMCondition(projArg.expression, schema)
      val dataUnion = new GProMListCell.data_union.ByValue(projGpromOperator.getPointer)
      listCell = new GProMListCell.ByReference()
      if(i==0)
        listTail
      listCell.data = dataUnion
      listCell.next = lastListCell
      lastListCell = listCell;
      i+=1
    }
    val projExprList = new GProMList.ByReference();
    projExprList.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
    projExprList.length = cols.length
    projExprList.head = listCell
    projExprList.tail = listTail
    projExprList
  }
  
   def createGProMListCell(gpromDataNode:GProMStructure) : GProMListCell.ByReference = {
    val listCell = new GProMListCell.ByReference()
    val dataUnion = new GProMListCell.data_union.ByValue(gpromDataNode.getPointer())
    listCell.data = dataUnion
    listCell.next = null
    listCell;
  }
}