package mimir.gprom.algebra

import org.gprom.jdbc.jna._
import mimir.algebra._

object ProjectionArgVisibility extends Enumeration {
   val Visible = Value("Visible")
   val Invisible = Value("Invisible") 
} 

object OperatorTranslation {
  
  def gpromStructureToMimirOperator(depth : Int, topOperator: Operator, gpromStruct: GProMStructure, gpromParentStruct: GProMStructure ) : Operator = {
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
        gpromStructureToMimirOperator(depth, topOperator, listHead, gpromParentStruct)
      }
      case listCell : GProMListCell => { 
        val listCellDataGPStructure = new GProMNode(listCell.data.ptr_value)
        val cnvNode = GProMWrapper.inst.castGProMNode(listCellDataGPStructure);
        if(topOp == null){
          topOp = gpromStructureToMimirOperator(depth, null, cnvNode, gpromParentStruct)
          if(listCell.next != null)
            gpromStructureToMimirOperator(depth, topOp, listCell.next, gpromParentStruct)
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
        val sourceChild = gpromStructureToMimirOperator(depth+1, topOp, projectionOperator.op, projectionOperator)
        val projArgs = getProjectionColumnsFromGProMProjectionOperator(projectionOperator)
        val visibleProjArgs = projArgs.map{ projArgT => projArgT._2 match { case ProjectionArgVisibility.Visible => Some(projArgT._1); case _ => None }}.flatten
        val invisibleProjArgs = projArgs.map{ projArgT => projArgT._2 match { case ProjectionArgVisibility.Invisible => Some(projArgT._1); case _ => None }}.flatten
        val invisibleSchema = projArgs.map{ projArgT => projArgT._2 match { case ProjectionArgVisibility.Invisible => Some((projArgT._1, projArgT._3, projArgT._4)); case _ => None }}.flatten
        val provAttrsLength = projectionOperator.op.provAttrs.length
         
        if(provAttrsLength > 0 && depth == 0){
          new Recover(new Project(visibleProjArgs, sourceChild), invisibleSchema)
        }
        else if(provAttrsLength > 0 ){
          new Project(visibleProjArgs, new Annotate(sourceChild, invisibleSchema))
        }
        else
          new Project(visibleProjArgs, sourceChild)
      }
      case provenanceComputation : GProMProvenanceComputation => { 
        null 
        }
      case provenanceTransactionInfo : GProMProvenanceTransactionInfo => { 
        null 
        }
      case queryOperator : GProMQueryOperator => { 
        queryOperator.`type` match {
          case GProM_JNA.GProMNodeTag.GProM_T_ProjectionOperator => gpromStructureToMimirOperator(depth+1, topOp,queryOperator.inputs, queryOperator)
          case GProM_JNA.GProMNodeTag.GProM_T_SelectionOperator => gpromStructureToMimirOperator(depth+1, topOp,queryOperator.inputs, queryOperator)
          case _ => gpromStructureToMimirOperator(depth+1, topOp,queryOperator.inputs, queryOperator)
        }
      }
      case schema : GProMSchema => { 
        null 
        }
      case selectionOperator : GProMSelectionOperator => { 
          val condition = translateGProMConditionToMimirExpression(selectionOperator.cond) 
          val sourceChild = gpromStructureToMimirOperator(depth+1, topOp, selectionOperator.op, selectionOperator)
          new Select(condition, sourceChild)
        }
      case setOperator : GProMSetOperator => { 
        null 
        }
      case tableAccessOperator : GProMTableAccessOperator => { 
          val tableSchema = getSchemaFromGProMQueryOperator(tableAccessOperator.tableName, tableAccessOperator.op)
          val tableMeta = Seq[(String,Expression,Type)]() //tableSchema.map(tup => (tup._1,null,tup._2))
          new Table(tableAccessOperator.tableName, tableSchema, tableMeta)
      }
      case updateOperator : GProMUpdateOperator => { 
        null 
        }
      case _ => { 
        null 
        }
    }
    
  }
  
  def translateGProMConditionToMimirExpression(gpromCond : GProMNode) : Expression = {
     val conditionNode = GProMWrapper.inst.castGProMNode(gpromCond)
     conditionNode match {
       case operator : GProMOperator => {
         var operatorEnumTypeValue = operator.name match {
            case "+" => (Arithmetic, Arith.Add)
            case "-" => (Arithmetic, Arith.Sub)
            case "*" => (Arithmetic, Arith.Mult)
            case "/" => (Arithmetic, Arith.Div)
            case "&" => (Arithmetic, Arith.And)
            case "|" => (Arithmetic, Arith.Or)
            case "AND" => (Arithmetic, Arith.And)
            case "OR" => (Arithmetic, Arith.Or)
            case "=" => (Comparison, Cmp.Eq )
            case "<>" => (Comparison, Cmp.Neq) 
            case ">" => (Comparison, Cmp.Gt )
            case ">=" => (Comparison, Cmp.Gte) 
            case "<" => (Comparison, Cmp.Lt) 
            case "<=" => (Comparison, Cmp.Lte )
            case "LIKE" => (Comparison, Cmp.Like) 
            case "NOT LIKE" => (Comparison, Cmp.NotLike)
            case x => throw new Exception("Invalid operand '"+x+"'")
        }
         
         val expressions = gpromListToScalaList(operator.args).toArray
         operatorEnumTypeValue._1 match {
           case Comparison => {
             new Comparison( operatorEnumTypeValue._2.asInstanceOf[Cmp.Op], translateGProMStructureToMimirExpression(expressions(0)), translateGProMStructureToMimirExpression(expressions(1)))
           }
           case Arithmetic => {
             new Arithmetic( operatorEnumTypeValue._2.asInstanceOf[Arith.Op], translateGProMStructureToMimirExpression(expressions(0)), translateGProMStructureToMimirExpression(expressions(1)))
           }
         }
       }
       case _ => null
     }
  }
  
  def translateGProMStructureToMimirExpression(gpromStruct : GProMStructure) : Expression = {
    gpromStruct match {
      case operator : GProMOperator => {
        translateGProMConditionToMimirExpression(new GProMNode(operator.getPointer()))
      }
      case attributeReference : GProMAttributeReference => {
        new Var(attributeReference.name)
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
      	null
      }
      case caseWhen : GProMCaseWhen => {
      	null
      }
      case castExpr : GProMCastExpr => {
      	null
      }
      case functionCall : GProMFunctionCall => {
      	null
      }
      case isNullExpr : GProMIsNullExpr => {
      	null
      }
      case orderExpr : GProMOrderExpr => {
      	null
      }
      case rowNumExpr : GProMRowNumExpr => {
      	null
      }
      case sQLParameter : GProMSQLParameter => {
      	null
      }
      case windowBound : GProMWindowBound => {
      	null
      }
      case windowDef : GProMWindowDef => {
      	null
      }
      case windowFrame : GProMWindowFrame => {
      	null
      }
      case windowFunction : GProMWindowFunction => {
      	null
      }
      case _ => {
        null
      }
    }
  }
  
  def getInvisibleProvenanceSchemaFromGProMQueryOperator(queryOperator : GProMQueryOperator) : Seq[(String, Type)] = {
    val provAttrs = gpromIntPointerListToScalaList(queryOperator.provAttrs)
    getSchemaFromGProMQueryOperator(/*tableAccessOperator.tableName*/"", queryOperator).zipWithIndex.map{ case (attr, index) => {
          if(provAttrs.contains(index))
            Some(attr)
          else
            None
        }
      }.flatten
  }
  
  def gpromListToScalaList(list: GProMList) : List[GProMStructure] = {
    var listCell = list.head
    (for(i <- 1 to list.length ) yield {
      val projInput = GProMWrapper.inst.castGProMNode(new GProMNode(listCell.data.ptr_value))
      listCell = listCell.next
      projInput
    }).toList
  }
  
  def gpromIntPointerListToScalaList(list: GProMList) : List[Int] = {
    var listCell = list.head
    (for(i <- 1 to list.length ) yield {
      val projInput = listCell.data.int_value
      listCell = listCell.next
      projInput
    }).toList
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
    var projTableName = ""
    var tableName = ""
    for(i <- 1 to projOpInputs.length ){
      val projInput = GProMWrapper.inst.castGProMNode(new GProMNode(listCell.data.ptr_value))
      tableName = projInput match {
        case tableAccessOperator : GProMTableAccessOperator => {
          projTableName = tableAccessOperator.tableName + "_"
          tableAccessOperator.tableName
        }
        case _ => {
          ""
        }
      }
      listCell = listCell.next
    }
    
    val projOpSchemaWithProv = getSchemaWithProvFromGProMQueryOperator(tableName, gpromProjOp.op)
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
            Some((new ProjectArg(attrRef.name, new Var(projOpSchemaT._1)), ProjectionArgVisibility.Visible, projOpSchemaT, tableName))//projTableName + attrRef.name)))
          else
            Some((new ProjectArg(attrRef.name, new Var(projOpSchemaT._1)), ProjectionArgVisibility.Invisible, projOpSchemaT, tableName))
        }
        case _ => None
      }
      listCell = listCell.next
      projArg
    }).flatten
    columns
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
  
  def getSchemaFromGProMQueryOperator(tableName : String, gpromQueryOp : GProMQueryOperator) : Seq[(String, Type)] = {
    val attrDefList = gpromQueryOp.schema.attrDefs;
    val tableNameStr = tableName match { 
        case "" => "" 
        case _ => tableName + "_" 
    }
    var listCell = attrDefList.head
    val columns : Seq[(String, Type)] =
    for(i <- 1 to attrDefList.length ) yield {
      val attrDef = new GProMAttributeDef(listCell.data.ptr_value)
      val attrType = getMimirTypeFromGProMDataType(attrDef.dataType)
      val schItem = ( tableNameStr +attrDef.attrName, attrType)
      listCell = listCell.next
      schItem
    }
    columns
  }
  
  def getSchemaWithProvFromGProMQueryOperator(tableName : String, gpromQueryOp : GProMQueryOperator) : (Seq[(String, Type)], List[Int]) = {
    val provAttrs = gpromIntPointerListToScalaList(gpromQueryOp.provAttrs)
    val attrDefList = gpromQueryOp.schema.attrDefs;
    val tableNameStr = tableName match { 
        case "" => "" 
        case _ => tableName + "_" 
    }
    var listCell = attrDefList.head
    val columns : Seq[(String, Type)] =
    for(i <- 1 to attrDefList.length ) yield {
      val attrDef = new GProMAttributeDef(listCell.data.ptr_value)
      val attrType = getMimirTypeFromGProMDataType(attrDef.dataType)
      var schItem : (String, Type) = null;
      if(!provAttrs.contains(i-1))
        schItem = ( tableNameStr +attrDef.attrName, attrType)
      else
        schItem = ( attrDef.attrName, attrType)
      listCell = listCell.next
      schItem
    }
    (columns, provAttrs)
  }
  
  
  def mimirOperatorToGProMStructure(mimirOperator :  Operator) : GProMStructure = {
    /*mimirOperator match {
			case Project(cols, src) => {
			  null
			 }
			case Recover(psel) => {
			  null
			}
			case Select(cond, src) => {
			  null
			}
			case Aggregate(groupBy, agggregates, source) => {
			  null
			}

			case Join(lhs, rhs) => { 
			  null
			}
			case LeftOuterJoin(lhs, rhs, condition) => {
			  null
			}
			case Table(name, sch, meta) => {
			  null
			}
		}*/
    mimirOperatorToGProMList(null, mimirOperator)
  }
  
  def mimirOperatorToGProMList(inList : GProMList.ByReference, mimirOperator :  Operator) : GProMList.ByReference = {
    var list  = inList
    if(list == null){
      list = new GProMList.ByReference()
      list.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
      list.length = 0;
    }
    mimirOperator match {
			case Project(cols, src) => {
			   val tableName = src match {
			     case Table(name, sch, meta) => {
			       name
			     }
			     case _ => ""
			   }
			    val toQoScm = translateMimirSchemaToGProMSchema(tableName, mimirOperator.schema)
			    val gqo = new GProMQueryOperator(GProM_JNA.GProMNodeTag.GProM_T_ProjectionOperator,  mimirOperatorToGProMList(null, src), toQoScm, null, null, null)  
			   list.head = createGProMListCell(new GProMProjectionOperator(gqo, translateMimirProjArgsToGProMList(mimirOperator.schema.map(a => a._1 -> a._2).toMap, cols))) 
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
			  val toQoScm = translateMimirSchemaToGProMSchema(name, sch)
			  val gqo = new GProMQueryOperator(GProM_JNA.GProMNodeTag.GProM_T_TableAccessOperator, new GProMList.ByReference(), toQoScm, null, null, null)
			  val gpromTable = new GProMTableAccessOperator(gqo,name,null)
			  list.head = createGProMListCell(gpromTable)
			  list.length += 1
			  list
			}

		}
  }
  
  def createGProMListCell(gpromDataNode:GProMStructure) : GProMListCell.ByReference = {
    val listCell = new GProMListCell.ByReference()
    gpromDataNode.write()
    val dataUnion = new GProMListCell.data_union(gpromDataNode.getPointer())
    dataUnion.write()
    listCell.data = dataUnion
    listCell.next = null
    listCell;
  }
  
  def getGProMDataTypeFromMimirType(mimirType : Type) : Int = {
    mimirType match {
        case TString() => GProM_JNA.GProMDataType.GProM_DT_VARCHAR2
        case TBool() => GProM_JNA.GProMDataType.GProM_DT_BOOL
        case TFloat() => GProM_JNA.GProMDataType.GProM_DT_FLOAT
        case TInt() => GProM_JNA.GProMDataType.GProM_DT_INT
        case _ => GProM_JNA.GProMDataType.GProM_DT_STRING
      }
  }
  
  def translateMimirSchemaToGProMSchema(tableName : String, schema : Seq[(String, Type)]) : GProMSchema.ByReference = {
    var listCell : GProMListCell.ByReference = null
    var lastListCell : GProMListCell.ByReference = null
    var listTail : GProMListCell.ByReference = null
    var i = 0;
    for(schemaTup : ((String, Type)) <- schema.reverse){
      val attrDef = new GProMAttributeDef(GProM_JNA.GProMNodeTag.GProM_T_AttributeDef, getGProMDataTypeFromMimirType(schemaTup._2 ), schemaTup._1.replaceFirst(tableName+"_", "")) 
      attrDef.write()
      val dataUnion = new GProMListCell.data_union(attrDef.getPointer)
      dataUnion.write()
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
    scmByRef.name = tableName
    scmByRef.attrDefs = attrDefList
    scmByRef
  }
  
  def translateMimirProjArgsToGProMList(schema : Map[String, Type], cols : Seq[ProjectArg]) : GProMList.ByReference = {
    var listCell : GProMListCell.ByReference = null
    var lastListCell : GProMListCell.ByReference = null
    var listTail : GProMListCell.ByReference = null
    var i = 0;
    for(projArg : ProjectArg <- cols.reverse){
      val attrRef = new GProMAttributeReference(GProM_JNA.GProMNodeTag.GProM_T_AttributeReference, projArg.name, 0, cols.length - i, 0, getGProMDataTypeFromMimirType(schema(projArg.name) )) 
      attrRef.write()
      val dataUnion = new GProMListCell.data_union(attrRef.getPointer)
      dataUnion.write()
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
}