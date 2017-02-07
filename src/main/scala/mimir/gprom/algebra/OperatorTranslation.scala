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
          val attrPrefix = extractAttrPrefixFromGProMOperator(selectionOperator)
          val condition = translateGProMConditionToMimirExpression(new GProMNode(selectionOperator.cond.getPointer), attrPrefix._2) 
          val sourceChild = gpromStructureToMimirOperator(depth+1, selectionOperator.op, selectionOperator)
          new Select(condition, sourceChild)
        }
      case setOperator : GProMSetOperator => { 
        throw new Exception("Translation Not Yet Implemented '"+setOperator+"'") 
        }
      case tableAccessOperator : GProMTableAccessOperator => { 
          val schemaNameAndAttrPrefix = extractAttrPrefixFromGProMOperator(tableAccessOperator)
          val tableSchema = getSchemaFromGProMQueryOperator(schemaNameAndAttrPrefix._1, tableAccessOperator.op)
          val tableMeta = Seq[(String,Expression,Type)]() //tableSchema.map(tup => (tup._1,null,tup._2))
          new Table(tableAccessOperator.tableName, tableSchema, tableMeta)
      }
      case updateOperator : GProMUpdateOperator => { 
        throw new Exception("Translation Not Yet Implemented '"+updateOperator+"'") 
        }
      case _ => { 
        null 
        }
    }
    
  }
  
  def translateGProMConditionToMimirExpression(gpromCond : GProMNode, attrPrefix : String) : Expression = {
     val conditionNode = GProMWrapper.inst.castGProMNode(gpromCond)
     conditionNode match {
       case operator : GProMOperator => {
         val expressions = gpromListToScalaList(operator.args).toArray
         operator.name match {
            case "+" => new Arithmetic( Arith.Add, translateGProMStructureToMimirExpression(expressions(0), attrPrefix), translateGProMStructureToMimirExpression(expressions(1), attrPrefix))
            case "-" => new Arithmetic( Arith.Sub, translateGProMStructureToMimirExpression(expressions(0), attrPrefix), translateGProMStructureToMimirExpression(expressions(1), attrPrefix))
            case "*" => new Arithmetic( Arith.Mult, translateGProMStructureToMimirExpression(expressions(0), attrPrefix), translateGProMStructureToMimirExpression(expressions(1), attrPrefix))
            case "/" => new Arithmetic( Arith.Div, translateGProMStructureToMimirExpression(expressions(0), attrPrefix), translateGProMStructureToMimirExpression(expressions(1), attrPrefix))
            case "&" => new Arithmetic( Arith.And, translateGProMStructureToMimirExpression(expressions(0), attrPrefix), translateGProMStructureToMimirExpression(expressions(1), attrPrefix))
            case "|" => new Arithmetic( Arith.Or, translateGProMStructureToMimirExpression(expressions(0), attrPrefix), translateGProMStructureToMimirExpression(expressions(1), attrPrefix))
            case "AND" => new Arithmetic( Arith.And, translateGProMStructureToMimirExpression(expressions(0), attrPrefix), translateGProMStructureToMimirExpression(expressions(1), attrPrefix))
            case "OR" => new Arithmetic( Arith.Or, translateGProMStructureToMimirExpression(expressions(0), attrPrefix), translateGProMStructureToMimirExpression(expressions(1), attrPrefix))
            case "=" => new Comparison( Cmp.Eq , translateGProMStructureToMimirExpression(expressions(0), attrPrefix), translateGProMStructureToMimirExpression(expressions(1), attrPrefix))
            case "<>" => new Comparison( Cmp.Neq, translateGProMStructureToMimirExpression(expressions(0), attrPrefix), translateGProMStructureToMimirExpression(expressions(1), attrPrefix))
            case ">" => new Comparison( Cmp.Gt, translateGProMStructureToMimirExpression(expressions(0), attrPrefix), translateGProMStructureToMimirExpression(expressions(1), attrPrefix)) 
            case ">=" => new Comparison( Cmp.Gte , translateGProMStructureToMimirExpression(expressions(0), attrPrefix), translateGProMStructureToMimirExpression(expressions(1), attrPrefix))
            case "<" => new Comparison( Cmp.Lt , translateGProMStructureToMimirExpression(expressions(0), attrPrefix), translateGProMStructureToMimirExpression(expressions(1), attrPrefix))
            case "<=" => new Comparison( Cmp.Lte , translateGProMStructureToMimirExpression(expressions(0), attrPrefix), translateGProMStructureToMimirExpression(expressions(1), attrPrefix))
            case "LIKE" => new Comparison( Cmp.Like , translateGProMStructureToMimirExpression(expressions(0), attrPrefix), translateGProMStructureToMimirExpression(expressions(1), attrPrefix))
            case "NOT LIKE" => new Comparison( Cmp.NotLike, translateGProMStructureToMimirExpression(expressions(0), attrPrefix), translateGProMStructureToMimirExpression(expressions(1), attrPrefix))
            case x => throw new Exception("Invalid operand '"+x+"'")
        }    
       }
       case x => throw new Exception("Expression Translation Not Yet Implemented '"+x+"'")
     }
  }
  
  def translateGProMStructureToMimirExpression(gpromStruct : GProMStructure, attrPrefix : String) : Expression = {
    gpromStruct match {
      case operator : GProMOperator => {
        translateGProMConditionToMimirExpression(new GProMNode(operator.getPointer()), attrPrefix)
      }
      case attributeReference : GProMAttributeReference => {
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
  
  def getInvisibleProvenanceSchemaFromGProMQueryOperator(queryOperator : GProMQueryOperator) : Seq[(String, Type)] = {
    val provAttrs = gpromIntPointerListToScalaList(queryOperator.provAttrs)
    val schemaNameAndAttrPrefix = extractAttrPrefixFromGProMOperator(queryOperator)
    getSchemaFromGProMQueryOperator(schemaNameAndAttrPrefix._1, queryOperator).zipWithIndex.map{ case (attr, index) => {
          if(provAttrs.contains(index))
            Some(attr)
          else
            None
        }
      }.flatten
  }
  
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
    var listCell = projOpInputs.head
    var schemaName = ""
    var attrPrefix = ""
    for(i <- 1 to projOpInputs.length ){
      val projInp = GProMWrapper.inst.castGProMNode(new GProMNode(listCell.data.ptr_value))
      val schemaNameAndAttrPrefix = extractAttrPrefixFromGProMOperator(projInp)
      schemaName = schemaNameAndAttrPrefix._1
      attrPrefix = schemaNameAndAttrPrefix._2
      listCell = listCell.next
    }
    
    val projOpSchemaWithProv = getSchemaWithProvFromGProMQueryOperator(schemaName, gpromProjOp.op)
    val projOpSchema = projOpSchemaWithProv._1.toArray
    val provAttrs = projOpSchemaWithProv._2
    
    listCell = projExprs.head
    val columns : Seq[(ProjectArg, ProjectionArgVisibility.Value, (String, Type), String)] =
    (for(i <- 1 to projExprs.length ) yield {
      val projExpr = GProMWrapper.inst.castGProMNode(new GProMNode(listCell.data.ptr_value))
      val projOpSchemaT = projOpSchema(i-1) 
      val projArg = projExpr match {
        case attrRef : GProMAttributeReference => {
          if(!provAttrs.contains(i-1))
            Some((new ProjectArg(attrRef.name, new Var(projOpSchemaT._1)), ProjectionArgVisibility.Visible, projOpSchemaT, schemaName))//projTableName + attrRef.name)))
          else
            Some((new ProjectArg(attrRef.name, new Var(projOpSchemaT._1)), ProjectionArgVisibility.Invisible, projOpSchemaT, schemaName))
        }
        case _ => None
      }
      listCell = listCell.next
      projArg
    }).flatten
    columns
  }
  
  def extractAttrPrefixFromGProMOperator(gpromOper : GProMStructure) : (String, String) = {
    gpromOper match {
        case tableAccessOperator : GProMTableAccessOperator => {
          val schemaName = tableAccessOperator.op.schema.name
          schemaName match { 
              case "" => {
                tableAccessOperator.tableName match { 
                  case "" => ("", "")
                  case _ => (tableAccessOperator.tableName, tableAccessOperator.tableName + "_")
                }  
              }
              case _ => (schemaName, schemaName + "_") 
          }
        }
        case projectionOperator : GProMProjectionOperator => { 
            extractAttrPrefixFromGProMOperator(projectionOperator.op.inputs)
          }
        case provenanceComputation : GProMProvenanceComputation => { 
          throw new Exception("Attribute Prefix Extraction  Not Yet Implemented '"+provenanceComputation+"'") 
          }
        case provenanceTransactionInfo : GProMProvenanceTransactionInfo => { 
          throw new Exception("Attribute Prefix Extraction  Not Yet Implemented '"+provenanceTransactionInfo+"'") 
          }
        case queryOperator : GProMQueryOperator => { 
            val schemaName = queryOperator.schema.name
            schemaName match { 
                case "" => ("", "")
                case _ => (schemaName, schemaName + "_") 
            }
          }
        case schema : GProMSchema => { 
            schema.name match { 
                case "" => ("", "")
                case _ => (schema.name, schema.name + "_") 
            }
          }
        case selectionOperator : GProMSelectionOperator => { 
            extractAttrPrefixFromGProMOperator(selectionOperator.op.inputs)
          }
        case list:GProMList => {
            val listHead = list.head
            extractAttrPrefixFromGProMOperator( listHead)
          }
          case listCell : GProMListCell => { 
            val listCellDataGPStructure = new GProMNode(listCell.data.ptr_value)
            val cnvNode = GProMWrapper.inst.castGProMNode(listCellDataGPStructure);
            val retOp = extractAttrPrefixFromGProMOperator(cnvNode)
            if(listCell.next != null)
               throw new Exception("There are more operators in the list but they are not being translated '"+listCell.next+"'") 
            retOp
          }
        case x => {
          throw new Exception("Attribute Prefix Extraction  Not Yet Implemented '"+x+"'") 
        }
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
  
  def getSchemaFromGProMQueryOperator(schemaName : String, gpromQueryOp : GProMQueryOperator) : Seq[(String, Type)] = {
    val attrDefList = gpromQueryOp.schema.attrDefs;
    //val schemaName = gpromQueryOp.schema.name
    val attrPrefix = schemaName match { 
        case "" => ""
        case _ => schemaName + "_"
    }
    var listCell = attrDefList.head
    val columns : Seq[(String, Type)] =
    for(i <- 1 to attrDefList.length ) yield {
      val attrDef = new GProMAttributeDef(listCell.data.ptr_value)
      val attrType = getMimirTypeFromGProMDataType(attrDef.dataType)
      val tableAndAttrName = attrPrefix +attrDef.attrName
      val schItem = ( tableAndAttrName, attrType)
      listCell = listCell.next
      schItem
    }
    columns
  }
  
  def getSchemaWithProvFromGProMQueryOperator(schemaName : String, gpromQueryOp : GProMQueryOperator) : (Seq[(String, Type)], List[Int]) = {
    val provAttrs = gpromIntPointerListToScalaList(gpromQueryOp.provAttrs)
    val attrDefList = gpromQueryOp.schema.attrDefs
    val attrPrefix = schemaName match { 
        case "" => ""
        case _ => schemaName + "_"
    }
    var listCell = attrDefList.head
    val columns : Seq[(String, Type)] =
    for(i <- 1 to attrDefList.length ) yield {
      val attrDef = new GProMAttributeDef(listCell.data.ptr_value)
      val attrType = getMimirTypeFromGProMDataType(attrDef.dataType)
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
  }
  
  
  def mimirOperatorToGProMList(mimirOperator :  Operator) : GProMList.ByReference = {
    mimirOperatorToGProMList(mimirOperator, extractTableSchemaForGProM(mimirOperator))
  }
  
  def mimirOperatorToGProMList(mimirOperator :  Operator, topSchema : Seq[((String, String, String), Type)]) : GProMList.ByReference = {
    val list = new GProMList.ByReference()
    list.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
    list.length = 0;
    
    mimirOperator match {
			case Project(cols, src) => {
			 //val schTable = extractTableSchemaForGProM(mimirOperator)
        val toQoScm = translateMimirSchemaToGProMSchema("PROJECTION", topSchema)
			  val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_ProjectionOperator,  mimirOperatorToGProMList(src, topSchema), toQoScm, null, null, null)
			  list.head = createGProMListCell(new GProMProjectionOperator.ByValue(gqo, translateMimirProjArgsToGProMList(topSchema, cols))) 
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
			  //val schTable = extractTableSchemaForGProM(mimirOperator)
        val toQoScm = translateMimirSchemaToGProMSchema("SELECT", topSchema)
        val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_SelectionOperator,  mimirOperatorToGProMList(src, topSchema), toQoScm, null, null, null) 
        val gpcond = translateMimirExpressionToGProMCondition(cond, topSchema) 
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
			case Table(name, sch, meta) => {
			  //val schTable = extractTableSchemaForGProM(mimirOperator)
        val schemaName =  topSchema.head._1._2 match {
			    case "" => name
			    case x => x
			  }
			  val toQoScm = translateMimirSchemaToGProMSchema(schemaName, topSchema)//mimirOperator)
			  val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_TableAccessOperator, null, toQoScm, null, null, null)
			  val gpromTable = new GProMTableAccessOperator.ByValue(gqo,name,null)
			  list.head = createGProMListCell(gpromTable)
			  list.length += 1 
			  list
			}

		}
  }
  
  def translateMimirExpressionToGProMCondition(mimirExpr : Expression, schema : Seq[((String, String, String), Type)]) : GProMOperator.ByValue = {
     
         var operatorEnumTypeValue = mimirExpr match {
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
              (cmpOp, list)
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
              (aritOp, list)
            }
            case x => {
              throw new Exception("Expression Translation not implemented '"+x+"'") 
            }
          }
         
         
         val gpop = new GProMOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_Operator, operatorEnumTypeValue._1,operatorEnumTypeValue._2)
         gpop
         
  }
  
  def translateMimirExpressionToGProMStructure(mimirExpr : Expression, schema : Seq[((String, String, String), Type)]) : GProMStructure = {
    mimirExpr match {
      case primitive : PrimitiveValue => translateMimirPrimitiveExpressionToGProMConstant(primitive)
      case Comparison(_,_,_) => translateMimirExpressionToGProMCondition(mimirExpr, schema)
      case Arithmetic(_,_,_) => translateMimirExpressionToGProMCondition(mimirExpr, schema)
      case Conditional(_,_,_) => translateMimirExpressionToGProMCondition(mimirExpr, schema)
      case Var(v) => {
        val indexOfCol = schema.map(ct => ct._1._3).indexOf(v)
        val attrRefNamePrefixPeeledOff = schema(indexOfCol)._1._2 match { 
              case "" => {
                schema(indexOfCol)._1._1 match { 
                  case "" => v
                  case x => v.replaceFirst(x+"_", "")
                }  
              }
              case x => v.replaceFirst(x+"_", "")
          }
        val attrRef = new GProMAttributeReference.ByValue(GProM_JNA.GProMNodeTag.GProM_T_AttributeReference, attrRefNamePrefixPeeledOff, 0, indexOfCol, 0, getGProMDataTypeFromMimirType(schema(indexOfCol)._2 ))
        attrRef
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
  
  def extractTableSchemaForGProM(oper : Operator) : Seq[((String, String, String), Type)] = {
    val originalSchema = oper.schema
    extractTableSchemaForGProM(oper, originalSchema)
  }
  
  def extractTableSchemaForGProM(oper : Operator, originalSchema : Seq[(String, Type)]) : Seq[((String, String, String), Type)] = {
    oper match {
      case Project(args, child) => {
        extractTableSchemaForGProM(child, originalSchema)
      }
      case Select(cond, source) => {
        extractTableSchemaForGProM(source, originalSchema)
      }
      case Join(lhs, rhs) => {
        extractTableSchemaForGProM(lhs, originalSchema).union(extractTableSchemaForGProM(rhs, originalSchema))
      }
      case LeftOuterJoin(lhs, rhs, cond) => {
        extractTableSchemaForGProM(lhs, originalSchema).union(extractTableSchemaForGProM(rhs, originalSchema))
      }
      case Table(name, tgtSch, metadata) => {
        oper.schema.zip(originalSchema).map(sch => ((name, sch._1._1.reverse.replaceFirst(sch._2._1.reverse, "").replaceFirst("_", "").reverse, sch._1._1.replaceFirst((name + "_"), "")), sch._1._2))
      }
      case x => {
        throw new Exception("Can Not extract schema '"+x+"'")
      }
    }
  }
  
  def translateMimirSchemaToGProMSchema(schemaName: String, oper : Operator) : GProMSchema.ByReference = {
    translateMimirSchemaToGProMSchema(schemaName, extractTableSchemaForGProM(oper)) 
  }
  
  def translateMimirSchemaToGProMSchema(schemaName: String, schema : Seq[((String, String, String), Type)]) : GProMSchema.ByReference = {
    var listCell : GProMListCell.ByReference = null
    var lastListCell : GProMListCell.ByReference = null
    var listTail : GProMListCell.ByReference = null
    var i = 0;
    
    
    for(schemaTup : ((String, String, String), Type) <- schema.reverse){
      val attrNamePrefixPeeledOff = schemaTup._1._2 match { 
              case "" => {
                schemaTup._1._1 match { 
                  case "" => schemaTup._1._3
                  case x => schemaTup._1._3.replaceFirst(x+"_", "")
                }  
              }
              case x => schemaTup._1._3.replaceFirst(x+"_", "")
          } 
      val attrDef = new GProMAttributeDef.ByValue(GProM_JNA.GProMNodeTag.GProM_T_AttributeDef, getGProMDataTypeFromMimirType(schemaTup._2 ), attrNamePrefixPeeledOff);
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
  
  def translateMimirProjArgsToGProMList(schema : Seq[((String, String, String), Type)], cols : Seq[ProjectArg]) : GProMList.ByReference = {
    var listCell : GProMListCell.ByReference = null
    var lastListCell : GProMListCell.ByReference = null
    var listTail : GProMListCell.ByReference = null
    var i = 0;
    val schemaIndex = schema.map(ct => ct._1._3);
    for(projArg : ProjectArg <- cols.reverse){
      val attrIndex = schemaIndex.indexOf(projArg.name)
      /*val attrNamePrefixPeeledOff = schema(attrIndex)._1._2 match { 
              case "" => {
                schema(attrIndex)._1._1 match { 
                  case "" => projArg.name
                  case x => projArg.name.replaceFirst(x+"_", "")
                }  
              }
              case x => projArg.name.replaceFirst(x+"_", "")
          }*/ 
      val attrRef = new GProMAttributeReference.ByValue(GProM_JNA.GProMNodeTag.GProM_T_AttributeReference, projArg.name, 0, (cols.length - i)-1, 0, getGProMDataTypeFromMimirType(schema(attrIndex)._2 ))
      val dataUnion = new GProMListCell.data_union.ByValue(attrRef.getPointer)
      listCell = new GProMListCell.ByReference()
      if(i==0)
        listTail
      listCell.data = dataUnion
      listCell.next = lastListCell
      lastListCell = listCell;
      i+=1
    }
    val attrRefList = new GProMList.ByReference();
    attrRefList.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
    attrRefList.length = cols.length
    attrRefList.head = listCell
    attrRefList.tail = listTail
    attrRefList
  }
  
   def createGProMListCell(gpromDataNode:GProMStructure) : GProMListCell.ByReference = {
    val listCell = new GProMListCell.ByReference()
    val dataUnion = new GProMListCell.data_union.ByValue(gpromDataNode.getPointer())
    listCell.data = dataUnion
    listCell.next = null
    listCell;
  }
}