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
  
  def gpromStructureToMimirOperator(depth : Int, topOperator: Operator, gpromStruct: GProMStructure, gpromParentStruct: GProMStructure ) : Operator = {
    var topOp = topOperator
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
              throw new Exception("Translation Not Yet Implemented '"+projectionOperator+"'") 
              }
            case provenanceComputation : GProMProvenanceComputation => { 
              throw new Exception("Translation Not Yet Implemented '"+provenanceComputation+"'") 
              }
            case provenanceTransactionInfo : GProMProvenanceTransactionInfo => { 
              throw new Exception("Translation Not Yet Implemented '"+provenanceTransactionInfo+"'") 
              }
            case queryOperator : GProMQueryOperator => { 
              throw new Exception("Translation Not Yet Implemented '"+queryOperator+"'") 
              }
            case schema : GProMSchema => { 
              throw new Exception("Translation Not Yet Implemented '"+schema+"'") 
              }
            case selectionOperator : GProMSelectionOperator => { 
              throw new Exception("Translation Not Yet Implemented '"+selectionOperator+"'") 
              }
          }
        }
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
        throw new Exception("Translation Not Yet Implemented '"+provenanceComputation+"'") 
        }
      case provenanceTransactionInfo : GProMProvenanceTransactionInfo => { 
        throw new Exception("Translation Not Yet Implemented '"+provenanceTransactionInfo+"'") 
        }
      case queryOperator : GProMQueryOperator => { 
        queryOperator.`type` match {
          case GProM_JNA.GProMNodeTag.GProM_T_ProjectionOperator => gpromStructureToMimirOperator(depth+1, topOp,queryOperator.inputs, queryOperator)
          case GProM_JNA.GProMNodeTag.GProM_T_SelectionOperator => gpromStructureToMimirOperator(depth+1, topOp,queryOperator.inputs, queryOperator)
          case _ => gpromStructureToMimirOperator(depth+1, topOp,queryOperator.inputs, queryOperator)
        }
      }
      case schema : GProMSchema => { 
        throw new Exception("Translation Not Yet Implemented '"+schema+"'") 
        }
      case selectionOperator : GProMSelectionOperator => { 
          val condition = translateGProMConditionToMimirExpression(selectionOperator.cond) 
          val sourceChild = gpromStructureToMimirOperator(depth+1, topOp, selectionOperator.op, selectionOperator)
          new Select(condition, sourceChild)
        }
      case setOperator : GProMSetOperator => { 
        throw new Exception("Translation Not Yet Implemented '"+setOperator+"'") 
        }
      case tableAccessOperator : GProMTableAccessOperator => { 
          val tableSchema = getSchemaFromGProMQueryOperator(tableAccessOperator.tableName, tableAccessOperator.op)
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
       case x => throw new Exception("Expression Translation Not Yet Implemented '"+x+"'")
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
			   val toQoScm = translateMimirSchemaToGProMSchema("PROJECTION", mimirOperator.schema)
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
			   //val srcSchemas = Typechecker.schemaOf(src)
			   /*val toQoScm = translateMimirSchemaToGProMSchema("SELECT", mimirOperator.schema)
			    val gqo = new GProMQueryOperator(GProM_JNA.GProMNodeTag.GProM_T_ProjectionOperator,  mimirOperatorToGProMList(null, src), toQoScm, null, null, null)  
			   list.head = createGProMListCell(new GProMSelectionOperator(gqo, )) 
			   list.length += 1;*/
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
  
  def translateMimirExpressionToGProMCondition(mimirExpr : Expression) : GProMStructure = {
     
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
               list.head = createGProMListCell(translateMimirExpressionToGProMStructure(lhs)) 
			         list.head.next = createGProMListCell(translateMimirExpressionToGProMStructure(rhs))      
               list.length = 2;
              (cmpOp, list)
            }
            case Arithmetic(op, lhs, rhs) => {
              val aritOp = op match {
                case  Arith.Add => "+" 
                case  Arith.Sub => "-" 
                case  Arith.Mult => "*" 
                case  Arith.Div => "/" 
                case  Arith.And => "&" 
                case  Arith.Or => "|" 
                case  Arith.And => "AND" 
                case  Arith.Or => "OR" 
                case x => throw new Exception("Invalid operand '"+x+"'")
              }
              val list = new GProMList.ByReference()
               list.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
               list.head = createGProMListCell(translateMimirExpressionToGProMStructure(lhs)) 
			         list.head.next = createGProMListCell(translateMimirExpressionToGProMStructure(rhs))      
               list.length = 2;
              (aritOp, list)
            }
            case x => throw new Exception("Expression Translation not implemented '"+x+"'") 
          }
         
         
         new GProMOperator(GProM_JNA.GProMNodeTag.GProM_T_Operator, operatorEnumTypeValue._1, operatorEnumTypeValue._2)
       
  }
  
  def translateMimirExpressionToGProMStructure(mimirExpr : Expression) : GProMStructure = {
    mimirExpr match {
      case primitive : PrimitiveValue => translateMimirPrimitiveExpressionToGProMConstant(primitive)
      case Comparison(_,_,_) => translateMimirExpressionToGProMCondition(mimirExpr)
      case Arithmetic(_,_,_) => translateMimirExpressionToGProMCondition(mimirExpr)
      case Conditional(_,_,_) => translateMimirExpressionToGProMCondition(mimirExpr)
    }
      /*GProMOperator 
      GProMAttributeReference 
      GProMConstant 
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
  
  def translateMimirPrimitiveExpressionToGProMConstant(mimirPrimitive : PrimitiveValue) : GProMConstant = {
    val typeValueIsNull = mimirPrimitive match {
      case IntPrimitive(v) => {
        val intPtr = new Memory(Native.getNativeSize(classOf[Int]));
        intPtr.setInt(0, v.asInstanceOf[Int]);
        (GProM_JNA.GProMDataType.GProM_DT_INT,intPtr,0)
      }
      case StringPrimitive(v) => {
        val strPtr = new Memory(v.length()+1);
        strPtr.setString(0, v);
        (GProM_JNA.GProMDataType.GProM_DT_STRING,strPtr,0)
      }
      case FloatPrimitive(v) => {
        val fltPtr = new Memory(Native.getNativeSize(classOf[Float]));
        fltPtr.setFloat(0, v.asInstanceOf[Float]);
        (GProM_JNA.GProMDataType.GProM_DT_FLOAT,fltPtr,0)
      }
      case RowIdPrimitive(v) => {
        throw new Exception("Primitive Expression Translation not implemented '"+v+"'")
      }
      case BoolPrimitive(v) => {
        val intPtr = new Memory(Native.getNativeSize(classOf[Int]));
        intPtr.setInt(0, v.asInstanceOf[Int]);
        (GProM_JNA.GProMDataType.GProM_DT_BOOL,intPtr,0)
      }
      case NullPrimitive() => {
        val intPtr = new Memory(Native.getNativeSize(classOf[Int]));
        intPtr.setInt(0,0);
        (GProM_JNA.GProMDataType.GProM_DT_INT,intPtr,1)
      }
      case DatePrimitive(y,m,d) => {
        val dtStr = ""+y+"-"+m+"-"+d
        val strPtr = new Memory(dtStr.length()+1);
        strPtr.setString(0, dtStr);
        (GProM_JNA.GProMDataType.GProM_DT_STRING,strPtr,0)
      }
    }
    new GProMConstant(GProM_JNA.GProMNodeTag.GProM_T_Constant, typeValueIsNull._1, typeValueIsNull._2, typeValueIsNull._3)
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
  
  def translateMimirSchemaToGProMSchema(schemaName: String, schema : Seq[(String, Type)]) : GProMSchema.ByReference = {
    var listCell : GProMListCell.ByReference = null
    var lastListCell : GProMListCell.ByReference = null
    var listTail : GProMListCell.ByReference = null
    val tableColType = schema.map( se => ( (se._1.split("_",2) match { 
        case Array(table,field) => (table,field) 
        case _ => ("", se._1)
      } ), se._2 ) )
    var i = 0;
    for(schemaTup : ((String, String), Type) <- tableColType.reverse){
      val attrDef = new GProMAttributeDef(GProM_JNA.GProMNodeTag.GProM_T_AttributeDef, getGProMDataTypeFromMimirType(schemaTup._2 ), schemaTup._1._2) 
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
    scmByRef.name = schemaName
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