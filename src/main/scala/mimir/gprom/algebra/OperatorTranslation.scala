package mimir.gprom.algebra

import org.gprom.jdbc.jna._

import com.sun.jna.Memory
import com.sun.jna.Native

import mimir.algebra._
import mimir.ctables.VGTerm
import mimir.ctables.VGTermAcknowledged
import mimir.sql.sqlite.VGTermFunctions
import mimir.exec.BestGuesser

object ProjectionArgVisibility extends Enumeration {
   val Visible = Value("Visible")
   val Invisible = Value("Invisible") 
} 

object OperatorTranslation {
  var db: mimir.Database = null
  def gpromStructureToMimirOperator(depth : Int, gpromStruct: GProMStructure, gpromParentStruct: GProMStructure ) : Operator = {
    gpromStruct match {
      case aggregationOperator : GProMAggregationOperator => { 
        val groupby = getGroupByColumnsFromGProMAggragationOperator(aggregationOperator)
        val aggregates = getAggregatesFromGProMAggragationOperator(aggregationOperator)
        val source = gpromStructureToMimirOperator(depth+1,aggregationOperator.op.inputs, aggregationOperator)
        Aggregate(groupby, aggregates, source)
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
        val lhs  = gpromStructureToMimirOperator(depth+1,joinOperator.op.inputs.head, joinOperator)
        val rhs = gpromStructureToMimirOperator(depth+1,joinOperator.op.inputs.head.next, joinOperator)
        val tableIntermSch = extractTableSchemaGProMOperator(joinOperator)
        val condition = translateGProMExpressionToMimirExpression(new GProMNode(joinOperator.cond.getPointer), tableIntermSch) 
        new Select(condition, new Join(lhs, rhs) )
        
        }
      case list:GProMList => {
        val listHead = list.head
        gpromStructureToMimirOperator(depth, listHead, gpromParentStruct)
      }
      case listCell : GProMListCell => { 
        val listCellDataGPStructure = new GProMNode(listCell.data.ptr_value)
        val cnvNode = GProMWrapper.inst.castGProMNode(listCellDataGPStructure);
        val retOp = gpromStructureToMimirOperator(depth, cnvNode, gpromParentStruct)
        /*if(listCell.next != null){
          val warningException = new Exception("There are more operators in the list but they are not being translated '"+listCell.next+"'") 
          println( warningException.toString() )
          warningException.printStackTrace()
        }*/
        retOp
      }
      case nestingOperator : GProMNestingOperator => { 
        throw new Exception("Translation Not Yet Implemented '"+nestingOperator+"'") 
        }
      case node : GProMNode => { 
        gpromStructureToMimirOperator(depth, GProMWrapper.inst.castGProMNode(node), gpromParentStruct)
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
          case GProM_JNA.GProMNodeTag.GProM_T_ProjectionOperator => gpromStructureToMimirOperator(depth+1, queryOperator.inputs, gpromParentStruct)
          case GProM_JNA.GProMNodeTag.GProM_T_SelectionOperator => gpromStructureToMimirOperator(depth+1, queryOperator.inputs, gpromParentStruct)
          case _ => gpromStructureToMimirOperator(depth+1, queryOperator.inputs, gpromParentStruct)
        }
      }
      case schema : GProMSchema => { 
        throw new Exception("Translation Not Yet Implemented '"+schema+"'") 
        }
      case selectionOperator : GProMSelectionOperator => { 
          val tableIntermSch = extractChildSchemaGProMOperator(selectionOperator)
          val condition = translateGProMExpressionToMimirExpression(new GProMNode(selectionOperator.cond.getPointer), tableIntermSch) 
          val sourceChild = gpromStructureToMimirOperator(depth+1, selectionOperator.op, selectionOperator)
          new Select(condition, sourceChild)
        }
      case setOperator : GProMSetOperator => { 
        throw new Exception("Translation Not Yet Implemented '"+setOperator+"'") 
        }
      case tableAccessOperator : GProMTableAccessOperator => { 
          val tableIntermSchema = extractTableSchemaGProMOperator(tableAccessOperator)
          val tableSchemap = tableIntermSchema.map(tis => (tis.getAttrPrefix()+tis.attrName, tis.attrType))//getSchemaFromGProMQueryOperator(tableIntermSchema, tableAccessOperator.op)
          val (tableSchema, tableMeta) = tableSchemap.map(el => {
            el._1 match {
              case "ROWID" => (None, Some(("MIMIR_ROWID", Var("ROWID"), TRowId())))
              case "MIMIR_ROWID" =>  (None, Some(("MIMIR_ROWID", Var("ROWID"), TRowId())))
              case _ => (Some(el), None)
            }
          }).unzip
          //val tableMeta = Seq[(String,Expression,Type)]() //tableSchema.map(tup => (tup._1,null,tup._2))
          new Table(tableAccessOperator.tableName, tableIntermSchema(0).alias, tableSchema.flatten, tableMeta.flatten)
      }
      case updateOperator : GProMUpdateOperator => { 
        throw new Exception("Translation Not Yet Implemented '"+updateOperator+"'") 
        }
      case _ => { 
        null 
        }
    }
    
  }
  
  def translateGProMExpressionToMimirExpression(gpromExpr : GProMNode, intermSchema : Seq[MimirToGProMIntermediateSchemaInfo]) : Expression = {
     val conditionNode = GProMWrapper.inst.castGProMNode(gpromExpr)
     conditionNode match {
       case operator : GProMOperator => {
         val expressions = gpromListToScalaList(operator.args).toArray
         operator.name match {
            case "+" => new Arithmetic( Arith.Add, translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "-" => new Arithmetic( Arith.Sub, translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "*" => new Arithmetic( Arith.Mult, translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "/" => new Arithmetic( Arith.Div, translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "&" => new Arithmetic( Arith.BitAnd, translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
            case "|" => new Arithmetic( Arith.BitOr, translateGProMStructureToMimirExpression(expressions(0), intermSchema), translateGProMStructureToMimirExpression(expressions(1), intermSchema))
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
  
  def translateGProMExpressionToMimirExpressionList(gpromExpr : GProMNode, intermSchema : Seq[MimirToGProMIntermediateSchemaInfo]) : Seq[Expression] = {
     val conditionNode = GProMWrapper.inst.castGProMNode(gpromExpr)
     conditionNode match {
        case list:GProMList => {
          val listHead = list.head
          translateGProMExpressionToMimirExpressionListHelper( listHead, intermSchema, Seq())
        }
       case x => Seq(translateGProMStructureToMimirExpression(conditionNode, intermSchema));
     }
  } 
  
  def translateGProMExpressionToMimirExpressionListHelper(gpromStruct : GProMStructure, intermSchema : Seq[MimirToGProMIntermediateSchemaInfo], exprsTail: Seq[Expression]) : Seq[Expression] = {
    gpromStruct match {
      case listCell : GProMListCell => { 
          val listCellDataGPStructure = new GProMNode(listCell.data.ptr_value)
          if(listCell.next != null){
            translateGProMExpressionToMimirExpressionListHelper(listCell.next,intermSchema, exprsTail.union(Seq(translateGProMExpressionToMimirExpression( listCellDataGPStructure, intermSchema))))
          }
          else
            exprsTail.union(Seq(translateGProMExpressionToMimirExpression( listCellDataGPStructure, intermSchema)))
        }
       case x => Seq(translateGProMStructureToMimirExpression(gpromStruct, intermSchema));
    }
  }
  
  
  def translateGProMCaseWhenListToMimirExpressionList(gpromStruct : GProMStructure, intermSchema : Seq[MimirToGProMIntermediateSchemaInfo], exprsTail: Seq[(Expression, Expression)]) : Seq[(Expression, Expression)] = {
    gpromStruct match {
      case listCell : GProMListCell => { 
          val listCellDataGPStructure = new GProMNode(listCell.data.ptr_value)
          if(listCell.next != null){
            translateGProMCaseWhenListToMimirExpressionList(listCell.next,intermSchema, exprsTail.union(Seq(translateGProMCaseWhenToMimirExpressions( listCellDataGPStructure, intermSchema))))
          }
          else
            exprsTail.union(Seq(translateGProMCaseWhenToMimirExpressions( GProMWrapper.inst.castGProMNode(listCellDataGPStructure), intermSchema)))
        }
       case x => {
          	throw new Exception("Expression not a list cell '"+x+"'")
          }
    }
  }
  
  def translateGProMCaseWhenToMimirExpressions(gpromStruct : GProMStructure, intermSchema : Seq[MimirToGProMIntermediateSchemaInfo]) :  (Expression, Expression) = {
    gpromStruct match {
      case caseWhen : GProMCaseWhen => {
          	(translateGProMExpressionToMimirExpression(caseWhen.when, intermSchema), translateGProMExpressionToMimirExpression(caseWhen.then, intermSchema))
          }
      case node : GProMNode => { 
        translateGProMCaseWhenToMimirExpressions( GProMWrapper.inst.castGProMNode(node),  intermSchema)
      }
      case x => {
          	throw new Exception("Expression not a case when clause '"+x+"'")
          }
    }
  }
  
  def translateGProMStructureToMimirExpression(gpromStruct : GProMStructure, intermSchema : Seq[MimirToGProMIntermediateSchemaInfo]) : Expression = {
    gpromStruct match {
      case operator : GProMOperator => {
        translateGProMExpressionToMimirExpression(new GProMNode(operator.getPointer()), intermSchema)
      }
      case attributeReference : GProMAttributeReference => {
        val attrMimirName = intermSchema.find(ise => ise.attrName.equals(attributeReference.name)) match {
          case None => attributeReference.name
          case Some(attrInterm) => attrInterm.attrMimirName
        }
        attrMimirName match {
          case "ROWID" => RowIdVar()
          case _ => new Var(attrMimirName)
        }
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
      	val whenThenClauses = translateGProMCaseWhenListToMimirExpressionList(caseExpr.whenClauses.head, intermSchema, Seq())
        val elseClause = translateGProMExpressionToMimirExpression(caseExpr.elseRes, intermSchema) 
        caseExpr.expr match {
      	  case null => {
      	    ExpressionUtils.makeCaseExpression(whenThenClauses.toList, elseClause)
      	  }
      	  case _ => {
      	    val testExpr = translateGProMExpressionToMimirExpression(caseExpr.expr, intermSchema) 
      	    ExpressionUtils.makeCaseExpression(testExpr, whenThenClauses, elseClause)
      	  }
      	}
        
      }
      case caseWhen : GProMCaseWhen => {
      	throw new Exception("You need to use translateGProMCaseWhenListToMimirExpressionList for '"+caseWhen+"'")
      }
      case castExpr : GProMCastExpr => {
        val castArgs = translateGProMExpressionToMimirExpressionList(castExpr.expr, intermSchema)
      	val fixedType = castArgs.last match {
          case IntPrimitive(i) => TypePrimitive(Type.toSQLiteType(i.toInt))
          case TypePrimitive(t) => TypePrimitive(t)
        }
        Function("CAST", Seq(castArgs.head,fixedType)  )
      }
      case functionCall : GProMFunctionCall => {
        functionCall.functionname match {
          case "NOT" => {
            Not(translateGProMExpressionToMimirExpression(new GProMNode(functionCall.args.head.data.ptr_value), intermSchema))
          }
          case "BEST_GUESS_VGTERM" => {
            val fargs = OperatorTranslation.gpromListToScalaList(functionCall.args).map(arg => translateGProMExpressionToMimirExpression(new GProMNode(arg.getPointer), intermSchema))
            val model = db.models.get(fargs(0).toString().replaceAll("'", ""))
            val idx = fargs(1).asInstanceOf[IntPrimitive].v.toInt;
            val vgtArgs =
              model.argTypes(idx).
                zipWithIndex.
                map( arg => fargs(arg._2+2))
            val vgtHints = 
              model.hintTypes(idx).
                zipWithIndex.
                map( arg => fargs(arg._2+vgtArgs.length+2))
            VGTerm(model, idx, vgtArgs, vgtHints)
          }
          case "ACKNOWLEDGED_VGTERM" => {
            val fargs = OperatorTranslation.gpromListToScalaList(functionCall.args).map(arg => translateGProMExpressionToMimirExpression(new GProMNode(arg.getPointer), intermSchema))
            val model = db.models.get(fargs(0).toString().replaceAll("'", ""))
            val idx = fargs(1).asInstanceOf[IntPrimitive].v.toInt;
            val vgtArgs =
              model.argTypes(idx).
                zipWithIndex.
                map( arg => fargs(arg._2+2))
            VGTermAcknowledged(model, idx, vgtArgs)
          }
          case "CAST" => {
            val castArgs = gpromListToScalaList(functionCall.args).map( gpromParam => translateGProMExpressionToMimirExpression(new GProMNode(gpromParam.getPointer), intermSchema))
          	val fixedType = castArgs.last match {
              case IntPrimitive(i) => TypePrimitive(Type.toSQLiteType(i.toInt))
              case TypePrimitive(t) => TypePrimitive(t)
              case x => x
            }
            Function("CAST", Seq(castArgs.head,fixedType)  )
          }
          case _ => {
            Function(functionCall.functionname, gpromListToScalaList(functionCall.args).map( gpromParam => translateGProMExpressionToMimirExpression(new GProMNode(gpromParam.getPointer), intermSchema)))
          }
        }
      }
      case isNullExpr : GProMIsNullExpr => {
      	IsNullExpression(translateGProMExpressionToMimirExpression(isNullExpr.expr, intermSchema))
      }
      case orderExpr : GProMOrderExpr => {
      	throw new Exception("Expression Translation Not Yet Implemented '"+orderExpr+"'")
      }
      case rowNumExpr : GProMRowNumExpr => {
      	RowIdVar()
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
  
  def getGProMAttributeReferenceAttrPosition(gpromExpr : GProMNode, intermSchema : Seq[MimirToGProMIntermediateSchemaInfo]) : Int = {
    val conditionNode = GProMWrapper.inst.castGProMNode(gpromExpr)
     conditionNode match {
      case operator : GProMOperator => {
         val expressions = gpromListToScalaList(operator.args).toArray
         getGProMAttributeReferenceAttrPosition(new GProMNode(expressions(0).getPointer), intermSchema);
      }
      case attributeReference : GProMAttributeReference => {
        attributeReference.attrPosition
      }
      case constant : GProMConstant => {
        0
      }
      case functionCall : GProMFunctionCall => {
        0
      }
      case caseExpr : GProMCaseExpr=> {
        0
      }
      case x => throw new Exception("get AttrPosition Not Yet Implemented for '"+x+"'")
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
      var scList = Seq[GProMStructure]()
      var listCell = list.head
      var i  = 1
      while(listCell != null){
        val projInput = GProMWrapper.inst.castGProMNode(new GProMNode(listCell.data.ptr_value))
        if(projInput == null)
          println("WTF... there is some issue this should not be null")
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
        projInput
        scList =  scList :+ projInput 
        i+=1
      }
      scList.toList
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
    val tableSchema = extractChildSchemaGProMOperator(gpromProjOp)
    val projSchema = getIntermediateSchemaFromGProMSchema(null,gpromProjOp.op.schema)
    
   val provAttrs = gpromIntPointerListToScalaList(gpromProjOp.op.provAttrs)
     
    var scList = Seq[(ProjectArg, ProjectionArgVisibility.Value, (String, Type), String)]()   
    var listCell = projExprs.head
    var i  = 1
    while(listCell != null){
      val projExpr = new GProMNode(listCell.data.ptr_value)
      val mimirExpr = translateGProMExpressionToMimirExpression(projExpr, tableSchema )
      val attrRefAttrPos = getGProMAttributeReferenceAttrPosition(projExpr, tableSchema)
      val projVisibility = {
        if(!provAttrs.contains(i-1))
          ProjectionArgVisibility.Visible
        else
          ProjectionArgVisibility.Invisible 
      }
      listCell = listCell.next
      scList = scList :+ (new ProjectArg(projSchema(i-1).attrProjectedName, mimirExpr), projVisibility, (projSchema(i-1).attrName, projSchema(i-1).attrType), tableSchema(attrRefAttrPos).alias)
      i+=1
    }
    scList.seq
  }
  
  def getGroupByColumnsFromGProMAggragationOperator(gpromAggOp : GProMAggregationOperator) : Seq[Var] = {
    val gropByExprs = gpromAggOp.groupBy;
      
    gropByExprs match {
      case null => Seq[Var]()
      case x => {  
        val aggOpInputs =  gpromAggOp.op.inputs
        val tableSchema = extractTableSchemaGProMOperator(aggOpInputs)
        val aggSchema = getIntermediateSchemaFromGProMSchema(null,gpromAggOp.op.schema)
    
        var scList = Seq[Var]()
        var listCell = gropByExprs.head
        var i = 1
        while(listCell != null) {
          val scmPrefix = tableSchema(i-1).getAttrPrefix()
          val groupByExpr = new GProMNode(listCell.data.ptr_value)
          val mimirExpr = translateGProMExpressionToMimirExpression(groupByExpr, tableSchema )
          listCell = listCell.next
          i+=1
          scList = scList :+ mimirExpr.asInstanceOf[Var]
        }
        scList.seq
      }
    }
  }
  
  def getAggregatesFromGProMAggragationOperator(gpromAggOp : GProMAggregationOperator) : Seq[AggFunction] = {
    val aggrs = gpromAggOp.aggrs;
    val gropByExprs = gpromAggOp.groupBy;
    val aggOpInputs =  gpromAggOp.op.inputs
    //val childSchema = extractChildSchemaGProMOperator(gpromAggOp)
    val aggSchema = getIntermediateSchemaFromGProMStructure(gpromAggOp)
    
    val projInputHead = new GProMNode(aggOpInputs.head.data.ptr_value)
    val arrgIntermSch = projInputHead.`type` match {
      case GProM_JNA.GProMNodeTag.GProM_T_ProjectionOperator => aggSchema
      case GProM_JNA.GProMNodeTag.GProM_T_TableAccessOperator => extractChildSchemaGProMOperator(gpromAggOp)
      case _ => aggSchema
    }
 
    var startIdx = 1
    if(gropByExprs != null)
      startIdx += gropByExprs.length
    
    if(aggrs == null)
      return Seq()
      
    var listCell = aggrs.head
    var i = 1;
    var aggFunctions = Seq[AggFunction]()
    while(listCell != null ) {
      //val scmPrefix = childSchema(i-1).getAttrPrefix()
      val aggr = new GProMFunctionCall(listCell.data.ptr_value)
      val aggrArgs = aggr.args
      var aggrArgListCell = aggrArgs.head
      val mimirAggrArgs = for(j <- 1 to aggrArgs.length ) yield {
        val aggrArg =  new GProMNode(aggrArgListCell.data.ptr_value)
        val mimirExpr = translateGProMExpressionToMimirExpression(aggrArg, arrgIntermSch )
        aggrArgListCell = aggrArgListCell.next
        mimirExpr
      }
      listCell = listCell.next
      aggFunctions = aggFunctions ++  Seq(new AggFunction(aggr.functionname, (aggr.isAgg == 0), mimirAggrArgs, aggSchema(i-1).attrMimirName))
      i+=1
    }
    aggFunctions
  }
  
  def extractTableSchemaGProMOperator(gpromOper : GProMStructure) : Seq[MimirToGProMIntermediateSchemaInfo] = {
    gpromOper match {
        case tableAccessOperator : GProMTableAccessOperator => {
          val schemaName = tableAccessOperator.op.schema.name
          val schemaNames = schemaName match { 
              case "" => {
                tableAccessOperator.tableName match { 
                  case "" => ("", "", "")
                  case _ => (tableAccessOperator.tableName, tableAccessOperator.tableName, "")//tableAccessOperator.tableName + "_")
                }  
              }
              case _ => (tableAccessOperator.tableName, schemaName, "")//schemaName + "_") 
          }
          var listCell = tableAccessOperator.op.schema.attrDefs.head
          (for(i <- 1 to tableAccessOperator.op.schema.attrDefs.length ) yield {
            val attrDef = new GProMAttributeDef(listCell.data.ptr_value)
            listCell = listCell.next
            new MimirToGProMIntermediateSchemaInfo(schemaNames._1, schemaNames._2, attrDef.attrName, schemaNames._3 + attrDef.attrName, "", getMimirTypeFromGProMDataType(attrDef.dataType), i-1, 0)
          }).toSeq
        }
        case projectionOperator : GProMProjectionOperator => { 
            extractTableSchemaGProMOperator(projectionOperator.op.inputs)
          }
        case aggragationOperator : GProMAggregationOperator => { 
          extractTableSchemaGProMOperator(aggragationOperator.op.inputs)
          }
        case provenanceComputation : GProMProvenanceComputation => { 
          throw new Exception("Table Schema Extraction Not Yet Implemented '"+provenanceComputation+"'") 
          }
        case provenanceTransactionInfo : GProMProvenanceTransactionInfo => { 
          throw new Exception("Table Schema Extraction  Not Yet Implemented '"+provenanceTransactionInfo+"'") 
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
        case joinOperator : GProMJoinOperator => { 
            extractTableSchemaGProMOperator(joinOperator.op.inputs)
          }
        case list:GProMList => {
            var listCell = list.head
            var reurnSeq = Seq[MimirToGProMIntermediateSchemaInfo]()
            for(i <- 1 to list.length ) {
              reurnSeq = joinIntermSchemas(reurnSeq, extractTableSchemaGProMOperator( listCell), 0, true)
              listCell = listCell.next
            }
            reurnSeq
          }
          case listCell : GProMListCell => { 
            val listCellDataGPStructure = new GProMNode(listCell.data.ptr_value)
            val cnvNode = GProMWrapper.inst.castGProMNode(listCellDataGPStructure);
            val retOp = extractTableSchemaGProMOperator(cnvNode)
            /*if(listCell.next != null){
              val warningException = new Exception("There are more operators in the list but they are not being translated '"+listCell.next+"'") 
              println( warningException.toString() )
              warningException.printStackTrace()
            }*/
            retOp
          }
        case x => {
          throw new Exception("Table Schema Extraction  Not Yet Implemented '"+x+"'") 
        }
      }
  }
  
 def extractChildSchemaGProMOperator(gpromOper : GProMStructure) : Seq[MimirToGProMIntermediateSchemaInfo] = {
    gpromOper match {
        case tableAccessOperator : GProMTableAccessOperator => {
            extractTableSchemaGProMOperator(tableAccessOperator)
          }
        case projectionOperator : GProMProjectionOperator => { 
            extractChildSchemaGProMOperator(projectionOperator.op.inputs)
          }
        case aggragationOperator : GProMAggregationOperator => { 
           extractChildSchemaGProMOperator(aggragationOperator.op.inputs)
          }
        case provenanceComputation : GProMProvenanceComputation => { 
          throw new Exception("Child Schema Extraction Not Yet Implemented '"+provenanceComputation+"'") 
          }
        case provenanceTransactionInfo : GProMProvenanceTransactionInfo => { 
          throw new Exception("Child Schema Extraction  Not Yet Implemented '"+provenanceTransactionInfo+"'") 
          }
        case selectionOperator : GProMSelectionOperator => { 
            extractChildSchemaGProMOperator(selectionOperator.op.inputs)
          }
        case joinOperator : GProMJoinOperator => { 
            extractChildSchemaGProMOperator(joinOperator.op.inputs)
          }
        case list:GProMList => {
            var listCell = list.head
            var reurnSeq = Seq[MimirToGProMIntermediateSchemaInfo]()
            for(i <- 1 to list.length ) {
              reurnSeq = joinIntermSchemas(reurnSeq, extractChildSchemaGProMOperator( listCell), 0, true)
              listCell = listCell.next
            }
            reurnSeq
          }
          case listCell : GProMListCell => { 
            val listCellDataGPStructure = new GProMNode(listCell.data.ptr_value)
            val cnvNode = GProMWrapper.inst.castGProMNode(listCellDataGPStructure);
            cnvNode match {
              case tableAccessOperator : GProMTableAccessOperator => extractTableSchemaGProMOperator(tableAccessOperator)
              case x => getIntermediateSchemaFromGProMStructure(cnvNode)
            }
          }
        case x => {
          throw new Exception("Child Schema Extraction Not Yet Implemented '"+x+"'") 
        }
      }
  }
 
  def getIntermediateSchemaFromGProMStructure(gpromOper: GProMStructure) : Seq[MimirToGProMIntermediateSchemaInfo] = {
    gpromOper match {
        case tableAccessOperator : GProMTableAccessOperator => {
            getIntermediateSchemaFromGProMSchema(null,tableAccessOperator.op.schema)
          }
        case projectionOperator : GProMProjectionOperator => { 
            getIntermediateSchemaFromGProMSchema(null,projectionOperator.op.schema)
          }
        case aggragationOperator : GProMAggregationOperator => { 
            getIntermediateSchemaFromGProMSchema(null,aggragationOperator.op.schema)
          }
        case provenanceComputation : GProMProvenanceComputation => { 
          throw new Exception("Table Schema Extraction Not Yet Implemented '"+provenanceComputation+"'") 
          }
        case provenanceTransactionInfo : GProMProvenanceTransactionInfo => { 
          throw new Exception("Table Schema Extraction  Not Yet Implemented '"+provenanceTransactionInfo+"'") 
          }
        case selectionOperator : GProMSelectionOperator => { 
            val tableSchema = extractTableSchemaGProMOperator(selectionOperator.op.inputs)
            getIntermediateSchemaFromGProMSchema((tableSchema(0).name,tableSchema(0).alias), selectionOperator.op.schema)
          }
        case joinOperator : GProMJoinOperator => { 
            extractTableSchemaGProMOperator(joinOperator.op.inputs)
          }
        case x => {
          throw new Exception("Schema Extraction Not Yet Implemented '"+x+"'") 
        }
      }
  }
 
 
  val aggOpNames = Seq("SUM", "COUNT")
  val exprOps = Seq("+", "-")
  
  def getIntermediateSchemaFromGProMSchema(tableNameAlias : (String, String), gpromSchema: GProMSchema) : Seq[MimirToGProMIntermediateSchemaInfo] = {
    val schemaNames = tableNameAlias match {
      case null | ("","") => gpromSchema.name match {
        case "" => ("", "", "")
        case _ => (gpromSchema.name, "", "") 
      }
      case (tn, ta) =>  (tn, ta, "")//ta + "_") 
    }
    var exprNoAliasCount = 0
    var firstExprNoAliasIndex = 0
    var aggrNoAliasCounts = Map[String, Int]()
    var firstAggrNoAliasIndexs = Map[String, Int]()
   
    var listCell = gpromSchema.attrDefs.head
    val intermSch = (for(i <- 1 to gpromSchema.attrDefs.length ) yield {
      val attrDef = new GProMAttributeDef(listCell.data.ptr_value)
      listCell = listCell.next
      //TODO: Replace this way of detecting Expression Attributes without aliases from GProM schema
      val exprNoAliasPattern = ("((?:.+)?(?:"+exprOps.mkString("\\", "|\\", "")+").*)").r
      var attrProjName = attrDef.attrName match {
          case exprNoAliasPattern(expr) => {
            exprNoAliasCount+=1
            if(exprNoAliasCount == 1){
              firstExprNoAliasIndex = i  
              "EXPR"
            }
            else
              "EXPR_" + (exprNoAliasCount-1)  
          }
          case x => x
      }
      if(attrProjName.equals(attrDef.attrName)){
        val aggrNoAliasPattern = ("(?:.+)?("+aggOpNames.mkString("\\b", "|\\b", "")+").*").r
        attrProjName = attrDef.attrName match {
          case aggrNoAliasPattern(aggrName) => {
            if(!aggrNoAliasCounts.contains(aggrName)){
              aggrNoAliasCounts += (aggrName -> 1)
              firstAggrNoAliasIndexs += (aggrName -> 1) 
              aggrName
            }
            else{
              aggrNoAliasCounts += (aggrName -> (aggrNoAliasCounts(aggrName)+1))
              aggrName + "_" + (aggrNoAliasCounts(aggrName)-1)   
            }
          }
          case x => x
      }
      }
      /*attrProjName match {
        case "ROWID" => "MIMIR_ROWID"
        case _ => attrProjName
      }*/
      new MimirToGProMIntermediateSchemaInfo(schemaNames._1, schemaNames._2, attrDef.attrName, schemaNames._3 + attrDef.attrName, attrProjName, getMimirTypeFromGProMDataType(attrDef.dataType), i-1, 0)
    }).toSeq
    if(exprNoAliasCount > 1){
      intermSch(firstExprNoAliasIndex-1).attrProjectedName = "EXPR_0"
    }
    for ((aggrName,aggrNoAliasCount) <- aggrNoAliasCounts){
      if(aggrNoAliasCount > 1){
        intermSch(firstAggrNoAliasIndexs(aggrName)-1).attrProjectedName = intermSch(firstAggrNoAliasIndexs(aggrName)-1).attrProjectedName+"_0"
      }
    }
    intermSch
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
  
  def joinIntermSchemas(intermSchema0 : Seq[MimirToGProMIntermediateSchemaInfo], intermSchema1 : Seq[MimirToGProMIntermediateSchemaInfo], fromClausePos : Int, offsetAttrPos : Boolean) : Seq[MimirToGProMIntermediateSchemaInfo] = {
    var attrPosOffset = 0
    if(offsetAttrPos)
      attrPosOffset = intermSchema0.length
    intermSchema0.union(intermSchema1.map(rise => new MimirToGProMIntermediateSchemaInfo(rise.name, rise.alias, rise.attrName, rise.attrMimirName, rise.attrProjectedName, rise.attrType, rise.attrPosition+attrPosOffset, fromClausePos)))
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
  
  class MimirToGProMIntermediateSchemaInfo(val name : String, val alias : String, val attrName : String, val attrMimirName : String, var attrProjectedName : String, val attrType : Type, val attrPosition : Int, val attrFromClausePosition : Int)  {
    def getAttrPrefix() : String = {
      alias match { 
          case "" => ""
          case x =>  ""//x + "_"
      }
    }
    override def toString() : String = {
      s"name: $name\nalias: $alias\nattrName: $attrName\nattrMimirName: $attrMimirName\nattrProjectedName: $attrProjectedName\nattrType: $attrType\nattrPosition: $attrPosition\nattrFromClausePosition: $attrFromClausePosition"
    }
  }
 /*( def mimirOperatorToGProMList(mimirOperator :  Operator) : GProMList.ByReference = {
    mimirOperatorToGProMList(mimirOperator, extractTableSchemaForGProM(mimirOperator))
  }*/
  
  def mimirOperatorToGProMList( mimirOperator :  Operator) : GProMList.ByReference = {
    val list = new GProMList.ByReference()
    list.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
    list.length = 0;
    
    mimirOperator match {
			case Project(cols, src) => {
  			 val schTable = extractTableSchemaForGProM(mimirOperator).union(src match {
            case Aggregate(_,_,_) => getSchemaForGProM(src)
            case _ => Seq[MimirToGProMIntermediateSchemaInfo]()
  			 })
  			 val schProj = getSchemaForGProM(mimirOperator)
  			 val schProjIn = getInSchemaForGProM(mimirOperator)
  			 
  			 val toQoScm = translateMimirSchemaToGProMSchema("PROJECTION", schProj)
  			 val gqoInputs = mimirOperatorToGProMList(src) 
  			 val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_ProjectionOperator,  gqoInputs, toQoScm, null, null, null)
  			 val gProjOp = new GProMProjectionOperator.ByValue(gqo, translateMimirProjArgsToGProMList(schProjIn, cols))
  			 setGProMQueryOperatorParentsList(gqoInputs,gProjOp)
  			 list.head = createGProMListCell(gProjOp) 
			   list.length += 1; 
			  list
			 }
			case ProvenanceOf(psel) => {
			  val schTable = extractTableSchemaForGProM(psel)
        val schProvIn = getSchemaForGProM(psel)
			  val gpProvSch = joinIntermSchemas(schProvIn, schTable.map(rise => new MimirToGProMIntermediateSchemaInfo( rise.name, rise.alias, "PROV_" +rise.attrName, "PROV_" +rise.attrMimirName, "PROV_" +rise.attrMimirName, rise.attrType, rise.attrPosition, 0)), 0, false)
        val toQoScm = translateMimirSchemaToGProMSchema("PROVENANCE", gpProvSch)
			  val gqoInputs = mimirOperatorToGProMList(psel)
  			 val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_ProvenanceComputation, gqoInputs, toQoScm, null, null, null)
			  val provCompOp = new GProMProvenanceComputation.ByValue(gqo, 0, 0, null, null)
			  list.head = createGProMListCell(provCompOp) 
			  list.length += 1; 
			  list
			}
			case Annotate(subj,invisScm) => {
        list
      }
			case Recover(subj,invisScm) => {
        list
      }
			
			case Aggregate(groupBy, agggregates, source) => {
			  val schTable = extractTableSchemaForGProM(mimirOperator)
        val schAggIn = getSchemaForGProM(source)
			  val gpromAggrs = translateMimirAggregatesToGProMList(schTable.union(schAggIn), agggregates)
			  val gpromGroupBy = translateMimirGroupByToGProMList(schTable.union(schAggIn), groupBy)
        val schAggr = getSchemaForGProM(mimirOperator)
  			val toQoScm = translateMimirSchemaToGProMSchema("AGG", schAggr)
			  val gqoInputs = mimirOperatorToGProMList(source)
  			 val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_AggregationOperator, gqoInputs, toQoScm, null, null, null)
			  val aggOp = new GProMAggregationOperator.ByValue(gqo, gpromAggrs, gpromGroupBy)
			  setGProMQueryOperatorParentsList(gqoInputs,aggOp)
			  list.head = createGProMListCell(aggOp) 
			  list.length += 1; 
			  list
			}

			case Select(cond, Join(lhs, rhs)) => { 
			  val leftList = mimirOperatorToGProMList(lhs)
			  val rightList = mimirOperatorToGProMList(rhs)
			  val schTable = joinIntermSchemas(getSchemaForGProM(lhs), getSchemaForGProM(rhs), 1, false)
        val toQoScm = translateMimirSchemaToGProMSchema("JOIN", schTable)
			  val joinInputsList = joinGProMLists(leftList, rightList) 
        val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_JoinOperator, joinInputsList, toQoScm, null, null, null)
			  val gpcond = translateMimirExpressionToGProMCondition(cond, schTable) 
        val gpnbr = new GProMNode.ByReference(gpcond.getPointer)
			  val gpjoinop = new GProMJoinOperator.ByValue(gqo, GProM_JNA.GProMJoinType.GProM_JOIN_INNER, gpnbr)
			  setGProMQueryOperatorParentsList(leftList,gpjoinop)
			  setGProMQueryOperatorParentsList(rightList,gpjoinop)
			  list.head = createGProMListCell(gpjoinop) 
			  list.length += 1; 
			  list
			}
			case Select(cond, src) => {
			  val schTable = getSchemaForGProM(mimirOperator)
        val toQoScm = translateMimirSchemaToGProMSchema("SELECT", schTable)
        val gqoInputs = mimirOperatorToGProMList(src)
  			val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_SelectionOperator, gqoInputs, toQoScm, null, null, null) 
        val gpcond = translateMimirExpressionToGProMCondition(cond, schTable) 
        val gpnbr = new GProMNode.ByReference(gpcond.getPointer)
			  val gpselop = new GProMSelectionOperator.ByValue(gqo, gpnbr )
			  setGProMQueryOperatorParentsList(gqoInputs,gpselop)
			  list.head = createGProMListCell(gpselop) 
			  list.length += 1; 
			  list
			}
			case LeftOuterJoin(lhs, rhs, condition) => {
			  list
			}
			case Limit(offset, limit, query) => {
			  println("TODO: handle limit translation....  but how?")
			  mimirOperatorToGProMList(query)
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
			case View(name, query, meta) => {
			  mimirOperatorToGProMList(query)
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
                case  Arith.BitAnd => "&" 
                case  Arith.BitOr => "|" 
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
            case Conditional(cond, thenClause, elseClause) => {
              val whenThen = new GProMCaseWhen.ByValue(GProM_JNA.GProMNodeTag.GProM_T_CaseWhen, new GProMNode.ByReference(translateMimirExpressionToGProMStructure(cond, schema).getPointer), new GProMNode.ByReference(translateMimirExpressionToGProMStructure(thenClause, schema).getPointer))
              val list = new GProMList.ByReference()
               list.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
               list.head = createGProMListCell(whenThen) 
			         list.length = 1;
               new GProMCaseExpr.ByValue(GProM_JNA.GProMNodeTag.GProM_T_CaseExpr,null, list, new GProMNode.ByReference(translateMimirExpressionToGProMStructure(elseClause, schema).getPointer))
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
        var indexOfCol = schema.map(ct => ct.attrMimirName).indexOf(v)
          if(indexOfCol < 0)
            indexOfCol = schema.map(ct => ct.attrName).indexOf(v)
            if(indexOfCol < 0)
              indexOfCol = schema.map(ct => ct.attrProjectedName).indexOf(v)
              if(indexOfCol < 0)
                /*indexOfCol = schema.map(ct => ct.attrMimirName).indexOf(v.replaceFirst(schema(0).alias+"_", ""))
                if(indexOfCol < 0)*/
                if(v.equals("MIMIR_ROWID_0")){
                   indexOfCol = schema.map(ct => ct.attrName).indexOf("MIMIR_ROWID")
                   if(indexOfCol < 0)
                      throw new Exception(s"Missing Var $v: not found in : ${schema.mkString(",")}")
                }
                else
                  throw new Exception(s"Missing Var $v: not found in : ${schema.mkString(",")}")
                
        val attrRef = new GProMAttributeReference.ByValue(GProM_JNA.GProMNodeTag.GProM_T_AttributeReference, v, schema(indexOfCol).attrFromClausePosition, schema(indexOfCol).attrPosition, 0, getGProMDataTypeFromMimirType(schema(indexOfCol).attrType ))
        attrRef
      }
      case RowIdVar() => {
        val ridexpr = new GProMRowNumExpr.ByValue(GProM_JNA.GProMNodeTag.GProM_T_RowNumExpr)
        ridexpr
      }
      /*case Function("CAST", params) => {
        val paramnew = params match {
          case x :: StringPrimitive(s) :: Nil => Seq[Expression](x, TypePrimitive(Type.toSQLiteType(Integer.parseInt(params(1).toString()))) )
          case x :: IntPrimitive(i) :: Nil   =>  Seq[Expression](x, TypePrimitive(Type.toSQLiteType(i.toInt)) )
  	      case x :: TypePrimitive(t)    :: Nil => Seq[Expression](x, TypePrimitive(t)  )
        }
        val gpromExprList = translateMimirExpressionsToGProMList(schema, paramnew)
        val gpromFunc = new GProMFunctionCall.ByValue(GProM_JNA.GProMNodeTag.GProM_T_FunctionCall, "CAST", gpromExprList, 0)
        gpromFunc
      }*/
      case Function(op, params) => {
        val gpromExprList = translateMimirExpressionsToGProMList(schema, params)
        val gpromFunc = new GProMFunctionCall.ByValue(GProM_JNA.GProMNodeTag.GProM_T_FunctionCall, op, gpromExprList, 0)
        gpromFunc
      }
      case VGTerm(model, idx, args, hints) => {
        val gpromExprList = translateMimirExpressionsToGProMList(schema, Seq(StringPrimitive(model.name), IntPrimitive(idx)).union(args.union(hints)))
        val gpromFunc = new GProMFunctionCall.ByValue(GProM_JNA.GProMNodeTag.GProM_T_FunctionCall, VGTermFunctions.bestGuessVGTermFn, gpromExprList, 0)
        gpromFunc
      }
      case VGTermAcknowledged(model, idx, args) => {
        val gpromExprList = translateMimirExpressionsToGProMList(schema, Seq(StringPrimitive(model.name), IntPrimitive(idx)).union(args))
        val gpromFunc = new GProMFunctionCall.ByValue(GProM_JNA.GProMNodeTag.GProM_T_FunctionCall, VGTermFunctions.acknowledgedVGTermFn, gpromExprList, 0)
        gpromFunc
      }
      case IsNullExpression(expr) => {
        val gpromExpr = translateMimirExpressionToGProMCondition(expr, schema);
        val gpromIsNullExpr = new GProMIsNullExpr.ByValue(GProM_JNA.GProMNodeTag.GProM_T_IsNullExpr, new GProMNode.ByReference(gpromExpr.getPointer))
        gpromIsNullExpr
      }
      case Not(expr) => {
        val gpromExprList = translateMimirExpressionsToGProMList(schema, Seq(expr))
        val gpromFunc = new GProMFunctionCall.ByValue(GProM_JNA.GProMNodeTag.GProM_T_FunctionCall, "NOT", gpromExprList, 0)
        gpromFunc
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
      --GProMFunctionCall 
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
        val intPtr = new Memory(Native.getNativeSize(classOf[Int]))
        intPtr.setInt(0, v.asInstanceOf[Int]);
        (GProM_JNA.GProMDataType.GProM_DT_INT,intPtr,0)
      }
      case StringPrimitive(v) => {
        val strPtr = new Memory(v.length()+1)
        strPtr.setString(0, v);
        (GProM_JNA.GProMDataType.GProM_DT_STRING,strPtr,0)
      }
      case FloatPrimitive(v) => {
        val fltPtr = new Memory(Native.getNativeSize(classOf[Float]))
        fltPtr.setFloat(0, v.asInstanceOf[Float]);
        (GProM_JNA.GProMDataType.GProM_DT_FLOAT,fltPtr,0)
      }
      case RowIdPrimitive(v) => {
        val strPtr = new Memory(v.length()+1)
        strPtr.setString(0, v);
        (GProM_JNA.GProMDataType.GProM_DT_STRING,strPtr,0)
        //throw new Exception("Primitive Expression Translation not implemented '"+v+"'")
      }
      case BoolPrimitive(v) => {
        val intPtr = new Memory(Native.getNativeSize(classOf[Int]))
        val boolToInt = if(v) 1; else 0;
        intPtr.setInt(0, boolToInt);
        (GProM_JNA.GProMDataType.GProM_DT_BOOL,intPtr,0)
      }
      case NullPrimitive() => {
        val intPtr = new Memory(Native.getNativeSize(classOf[Int]))
        intPtr.setInt(0,0);
        (GProM_JNA.GProMDataType.GProM_DT_INT,intPtr,1)
      }
      case DatePrimitive(y,m,d) => {
        val dtStr = ""+y+"-"+m+"-"+d
        val strPtr = new Memory(dtStr.length()+1)
        strPtr.setString(0, dtStr);
        (GProM_JNA.GProMDataType.GProM_DT_STRING,strPtr,0)
      }
      case TypePrimitive(t) => {
        val v = Type.id(t)
        val intPtr = new Memory(Native.getNativeSize(classOf[Int]))
        intPtr.setInt(0, v.asInstanceOf[Int]);
        (GProM_JNA.GProMDataType.GProM_DT_INT,intPtr,0)
      }
    }
    val gpc = new GProMConstant.ByValue(GProM_JNA.GProMNodeTag.GProM_T_Constant, typeValueIsNull._1, typeValueIsNull._2, typeValueIsNull._3)
    gpc
  }
  
  
  def translateMimirExpressionToStringForGProM(mimirExpr : Expression, schema : Seq[MimirToGProMIntermediateSchemaInfo]) : String = {
    mimirExpr match {
      case primitive : PrimitiveValue => primitive.toString()
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
        "("+translateMimirExpressionToStringForGProM(lhs, schema) + cmpOp + translateMimirExpressionToStringForGProM(rhs, schema)+")"
      }
      case Arithmetic(op, lhs, rhs) => {
        val aritOp = op match {
          case  Arith.Add => "+" 
          case  Arith.Sub => "-" 
          case  Arith.Mult => "*" 
          case  Arith.Div => "/" 
          case  Arith.BitAnd => "&" 
          case  Arith.BitOr => "|" 
          case  Arith.And => "AND" 
          case  Arith.Or => "OR" 
          case x => throw new Exception("Invalid operand '"+x+"'")
        }
        "("+translateMimirExpressionToStringForGProM(lhs, schema) + aritOp + translateMimirExpressionToStringForGProM(rhs, schema)+")"    
      }
      case Conditional(cond, thenClause, elseClause) => {
        "( IF "+translateMimirExpressionToStringForGProM(cond, schema) + " THEN " + translateMimirExpressionToStringForGProM(thenClause, schema) + " ELSE " + translateMimirExpressionToStringForGProM(elseClause, schema)+" END )"    
      }
      case Var(v) => {
        var indexOfCol = schema.map(ct => ct.attrMimirName).indexOf(v)
          if(indexOfCol < 0)
            /*indexOfCol = schema.map(ct => ct.attrName).indexOf(v)
            if(indexOfCol < 0)
              indexOfCol = schema.map(ct => ct.attrProjectedName).indexOf(v)
              if(indexOfCol < 0)*/
                //throw new Exception(s"Missing Var $v: not found in : ${schema.mkString(",")}")
            v
          else
            schema(indexOfCol).attrName
      }
      case RowIdVar() => {
        mimirExpr.toString()
      }
      case Function(op, params) => {
        params.map(param => translateMimirExpressionToStringForGProM(param, schema) ).mkString(s"$op(", ",", ")")
      }
      case VGTerm(model, idx, args, hints) => {
        Seq(StringPrimitive(model.name), IntPrimitive(idx)).union(args.union(hints)).map(param => translateMimirExpressionToStringForGProM(param, schema) ).mkString(s"${VGTermFunctions.bestGuessVGTermFn}(", ",", ")")
      }
      case VGTermAcknowledged(model, idx, args) => {
        Seq(StringPrimitive(model.name), IntPrimitive(idx)).union(args).map(param => translateMimirExpressionToStringForGProM(param, schema) ).mkString(s"${VGTermFunctions.acknowledgedVGTermFn}(", ",", ")")
      }
      case IsNullExpression(expr) => {
        "("+translateMimirExpressionToStringForGProM(expr, schema) + " IS NULL )" 
      }
      case Not(expr) => {
        "(NOT(" + translateMimirExpressionToStringForGProM(expr, schema)+"))"
      }
      case x => {
        throw new Exception("Expression Translation not implemented "+x.getClass.toString()+": '"+x+"'")
      }
    }
      /*//GProMOperator 
      //GProMAttributeReference 
      //GProMConstant 
      --GProMCaseExpr 
      --GProMCaseWhen 
      GProMCastExpr
      --GProMFunctionCall 
      GProMIsNullExpr 
      GProMOrderExpr
      GProMRowNumExpr
      GProMSQLParameter
      GProMWindowBound
      GProMWindowDef
      GProMWindowFrame
      GProMWindowFunction */
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
  
  //This commented section was a hack to rectify GProM-Mimir differences with Aggregation Naming convention 
  /*def getSchemaForGProMProjectedAggregateChild(oper : Operator) : Seq[MimirToGProMIntermediateSchemaInfo] = {
     oper match {
       case Aggregate(groupBy, agggregates, source) => {
         val tableSchema = extractTableSchemaForGProM(source)
         agggregates.zip(oper.schema).zipWithIndex.map(aggr => {
            val aggrOpNoAliasPattern = ("(("+ aggOpNames.mkString("\\b(?:MIMIR_AGG_)?", "|\\b(?:MIMIR_AGG_)?", "")+")(?:_[0-9]+)?)").r
            val attrProjName = aggr._1._2._1 match {
                case aggrOpNoAliasPattern(whole, opname) => aggFunctionToGProMName(aggr._1._1, tableSchema(0))
                case x => x.replaceFirst("MIMIR_AGG_", "")
            }
            val attrNameInfo = ("AGGR_"+aggr._2, aggr._1._2._1, attrProjName)
            new MimirToGProMIntermediateSchemaInfo(tableSchema(0).name, tableSchema(0).alias, attrNameInfo._1, attrNameInfo._2, attrNameInfo._3, aggr._1._2._2, aggr._2, 0)
          })
       }
       case x => {
        throw new Exception("Error: '"+x+"' is not an Aggregate")
      }
     }
  }
  
  def aggFunctionToGProMName(aggr : AggFunction, iSchI : MimirToGProMIntermediateSchemaInfo) : String = {
    val prefix = iSchI.alias match {
      case "" => ""
      case x => x + "_"
    }
    val strippedAggrArgs = aggr.args.map(aggrArg => {
      aggrArg.toString().replaceAll(prefix, "")
    })
    aggr.function.toString + strippedAggrArgs.mkString("(",",",")")
  }*/
  
  def getInSchemaForGProM(oper : Operator) : Seq[MimirToGProMIntermediateSchemaInfo] = {
    oper match {
      case Project(args, child) => {
        getSchemaForGProM(child )
      }
      case _ => getSchemaForGProM(oper )
    }
  }
  
  def getSchemaForGProM(oper : Operator) : Seq[MimirToGProMIntermediateSchemaInfo] = {
    oper match {
      case Project(args, child) => {
        val tableSchema = child match {
          case Aggregate(_,_,_) => getSchemaForGProM(child)//getSchemaForGProMProjectedAggregateChild(child) //This commented section was a hack to rectify GProM-Mimir differences with Aggregation Naming convention 
          case _ => extractTableSchemaForGProM(child)
        }
        //val tschMap = tableSchema.map(tsche => (tsche.attrName, tsche)).toMap
        val pargsOut = args.map(pa => pa.name)
        val pargsIn = args.map(pa => pa.expression.toString)
        val gpargsIn = args.map(pa =>  translateMimirExpressionToStringForGProM(pa.expression, tableSchema))
        val operSchema = db.bestGuessSchema(oper)       
        pargsOut.zipWithIndex.map( index_argOut => {
          val fse = tableSchema(0)
          val index = index_argOut._2
          val exprNoAliasPattern = "(EXPR(?:_[0-9]+)?)".r
          val attrProjName = index_argOut._1 match {
              case exprNoAliasPattern(mimirExprName) => gpargsIn(index)
              case x => x/*child match { // This commented section was a hack to rectify GProM-Mimir differences with Aggregation Naming convention 
                case Aggregate(_,_,_) => tableSchema(index_argOut._2).attrProjectedName
                case _ => x
              }*/
          }
          new MimirToGProMIntermediateSchemaInfo(fse.name,fse.alias,gpargsIn(index),pargsIn(index),attrProjName,operSchema(index)._2, index, 0)
        })
      }
      case Aggregate(groupBy, agggregates, source) => {
        //TODO: fix this hack for matching to gprom AGGR_{N} names to mimir MIMIR_AGG_{attrAlias}
        val tableSchema = extractTableSchemaForGProM(source)
        val aggrSchema = db.bestGuessSchema(oper)
        (groupBy.union(agggregates)).zip(aggrSchema).zipWithIndex.map(aggr => {
          val attrNameInfo = (aggr._1._2._1/*"AGGR_"+aggr._2*/, aggr._1._2._1, "")//aggFunctionToGProMName(aggr._1._1, tableSchema(0))
          new MimirToGProMIntermediateSchemaInfo(tableSchema(0).name, tableSchema(0).alias, attrNameInfo._1, attrNameInfo._2, attrNameInfo._3, aggr._1._2._2, aggr._2, 0)
        })
        //getSchemaForGProM(source)
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
      case Limit(_,_,query) => {
        getSchemaForGProM(query)
      }
      case Table(name, alias, tgtSch, metadata) => {
         db.bestGuessSchema(oper).zipWithIndex.map(sch => new MimirToGProMIntermediateSchemaInfo(name, alias, sch._1._1.replaceFirst((alias + "_"), ""), if(sch._1._1.startsWith(alias + "_")) sch._1._1 else alias + "_"+sch._1._1, "", sch._1._2, sch._2, 0))
      }
      case View(name, query, annotations) => {
        db.bestGuessSchema(oper).zipWithIndex.map(sch => new MimirToGProMIntermediateSchemaInfo(name, name, sch._1._1.replaceFirst((name + "_"), ""), if(sch._1._1.startsWith(name + "_")) sch._1._1 else name + "_"+sch._1._1, "", sch._1._2, sch._2, 0))
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
      case Aggregate(groupBy, agggregates, source) => {
         extractTableSchemaForGProM(source)
      }
      case Select(cond, source) => {
         extractTableSchemaForGProM(source)
      }
      case Join(lhs, rhs) => {
        joinIntermSchemas(extractTableSchemaForGProM(lhs), extractTableSchemaForGProM(rhs), 0, true)
      }
      case LeftOuterJoin(lhs, rhs, cond) => {
        joinIntermSchemas(extractTableSchemaForGProM(lhs), extractTableSchemaForGProM(rhs), 0, true)
      }
      case Limit(_,_,query) => {
        extractTableSchemaForGProM(query)
      }
      case Table(name, alias, tgtSch, metadata) => {
        db.bestGuessSchema(oper).zipWithIndex.map(sch => new MimirToGProMIntermediateSchemaInfo(name, alias, sch._1._1.replaceFirst((alias + "_"), ""), if(sch._1._1.startsWith(alias + "_")) sch._1._1 else alias + "_"+sch._1._1, "", sch._1._2, sch._2, 0))
      }
      case View(name, query, annotations) => {
        db.bestGuessSchema(oper).zipWithIndex.map(sch => new MimirToGProMIntermediateSchemaInfo(name, name, sch._1._1.replaceFirst((name + "_"), ""), if(sch._1._1.startsWith(name + "_")) sch._1._1 else name + "_"+sch._1._1, "", sch._1._2, sch._2, 0))
      }
      case x => {
        throw new Exception("Can Not extract schema "+x.getClass.toString()+": '"+x+"'")
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
  
  def translateMimirGroupByToGProMList(schema : Seq[MimirToGProMIntermediateSchemaInfo], groupBy : Seq[Var]) : GProMList.ByReference = {
    if(groupBy.isEmpty)
      null
    else{  
      translateMimirExpressionsToGProMList(schema, groupBy)
    }
  }
  
  def translateMimirAggregatesToGProMList(schema : Seq[MimirToGProMIntermediateSchemaInfo], agggregates : Seq[AggFunction]) : GProMList.ByReference = {
    val aggrsList = new GProMList.ByReference()
    aggrsList.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
    var i = 0
    var aggrListCell = aggrsList.head
    agggregates.foreach(aggr => {
       val gpromAggrArgs = new GProMList.ByReference()
      gpromAggrArgs.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
      var j = 0;
      var aggrArgListCell = gpromAggrArgs.head
      aggr.args.foreach(arg => {
        val gpromArg = translateMimirExpressionToGProMCondition(arg, schema)
        val newArgCell = createGProMListCell(gpromArg)
        if(j == 0){
        gpromAggrArgs.head = newArgCell
        aggrArgListCell = aggrsList.head
        }
        else{
          aggrArgListCell.next = newArgCell
          aggrArgListCell = aggrArgListCell.next
        }
        j+=1
        gpromAggrArgs.length += 1
      })
      val gpromAggr = new GProMFunctionCall.ByValue(GProM_JNA.GProMNodeTag.GProM_T_FunctionCall, aggr.function, gpromAggrArgs, 1)
      val newAggrCell = createGProMListCell(gpromAggr)
      if(i == 0){
        aggrsList.head = newAggrCell
        aggrListCell = aggrsList.head
      }
      else{
        aggrListCell.next = newAggrCell
        aggrListCell = aggrListCell.next
      }
      i+=1  
	    aggrsList.length += 1
    })
    aggrsList
  }
 
  def translateMimirExpressionsToGProMList(schema : Seq[MimirToGProMIntermediateSchemaInfo], exprs: Seq[Expression]) : GProMList.ByReference = {
    val gpromExprs = new GProMList.ByReference()
    gpromExprs.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
    var j = 0;
    var exprListCell = gpromExprs.head
    exprs.foreach(expr => {
      val gpromExpr = translateMimirExpressionToGProMCondition(expr, schema)
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
    gpromExprs
  }
  
  def setGProMQueryOperatorParentsList(subject : GProMStructure, parent:GProMStructure) : Unit = {
    subject match {
      case tableAccessOperator : GProMTableAccessOperator => {
          tableAccessOperator.op.parents = createGProMQueryOperatorParentsList(parent) 
          tableAccessOperator.write()
        }
      case projectionOperator : GProMProjectionOperator => { 
          projectionOperator.op.parents = createGProMQueryOperatorParentsList(parent)
          projectionOperator.write()
        }
      case aggragationOperator : GProMAggregationOperator => { 
         aggragationOperator.op.parents = createGProMQueryOperatorParentsList(parent)
         aggragationOperator.write()
        }
      case provenanceComputation : GProMProvenanceComputation => { 
          provenanceComputation.op.parents = createGProMQueryOperatorParentsList(parent)
          provenanceComputation.write()
        }
      case selectionOperator : GProMSelectionOperator => { 
          selectionOperator.op.parents = createGProMQueryOperatorParentsList(parent)
          selectionOperator.write()
        }
      case joinOperator : GProMJoinOperator => { 
          joinOperator.op.parents = createGProMQueryOperatorParentsList(parent)
          joinOperator.write()
        }
      case list:GProMList => {
          var listCell = list.head
          while(listCell != null) {
            setGProMQueryOperatorParentsList(listCell, parent)
            listCell = listCell.next
          }
        }
        case listCell : GProMListCell => { 
          val listCellDataGPStructure = new GProMNode.ByReference(listCell.data.ptr_value)
          val cnvNode = GProMWrapper.inst.castGProMNode(listCellDataGPStructure);
          setGProMQueryOperatorParentsList(cnvNode, parent)
        }
       case x => {
        throw new Exception("Setting Parents Not Implemented '"+x+"'") 
      }
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
    parentsList
  }
  
  def createGProMListCell(gpromDataNode:GProMStructure) : GProMListCell.ByReference = {
    val listCell = new GProMListCell.ByReference()
    val dataUnion = new GProMListCell.data_union.ByValue(gpromDataNode.getPointer())
    listCell.data = dataUnion
    listCell.next = null
    listCell
  }
   
   def joinGProMLists(list0 : GProMList.ByReference, list1 : GProMList.ByReference) : GProMList.ByReference = {
    val list = new GProMList.ByReference()
    list.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
    var listCell = list0.head
    list.head = listCell
    var newListCell = listCell
    for(i <- 1 to (list0.length -1) ) {
      listCell = listCell.next
      newListCell.next = listCell
      newListCell = newListCell.next
    }
    listCell = list1.head
    newListCell.next = listCell
    newListCell = newListCell.next
    for(i <- 1 to list1.length ) {
      listCell = listCell.next
      newListCell.next = listCell
      newListCell = newListCell.next
    }
    list.length = list0.length + list1.length;
    list
  }
  
   def optimizeWithGProM(oper:Operator) : Operator = {
    synchronized{
      db.backend.asInstanceOf[mimir.sql.GProMBackend].metadataLookupPlugin.setOper(oper)
        //val memctx = GProMWrapper.inst.gpromCreateMemContext()
        //val memctxq = GProMWrapper.inst.createMemContextName("QUERY_CONTEXT")
        val gpromNode = mimirOperatorToGProMList(oper)
        gpromNode.write()
        //val gpromNodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
        val optimizedGpromNode = GProMWrapper.inst.optimizeOperatorModel(gpromNode.getPointer)
        /*val optNodeStr = GProMWrapper.inst.gpromNodeToString(optimizedGpromNode.getPointer())
        println("------------------------------------------------")
        println(oper)
        println("------------------------------------------------")
        println(optNodeStr)
        println("------------------------------------------------")*/
        //Thread.sleep(500)
        val opOut = gpromStructureToMimirOperator(0, optimizedGpromNode, null)
        //GProMWrapper.inst.gpromFreeMemContext(memctx)
        opOut
    }
  }
   
   def compileProvenanceWithGProM(oper:Operator) : (Operator, Seq[String])  = {
    synchronized{
      db.backend.asInstanceOf[mimir.sql.GProMBackend].metadataLookupPlugin.setOper(oper)
        //val memctx = GProMWrapper.inst.gpromCreateMemContext()
        val memctxq = GProMWrapper.inst.createMemContextName("QUERY_CONTEXT")
        val gpromNode = mimirOperatorToGProMList(ProvenanceOf(oper))
        gpromNode.write()
        //val gpromNodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
        val provGpromNode = GProMWrapper.inst.provRewriteOperator(gpromNode.getPointer)
        
        val optNodeStr = GProMWrapper.inst.gpromNodeToString(provGpromNode.getPointer())
        println("------------------------------------------------")
        println(oper)
        println("------------------------------------------------")
        println(optNodeStr)
        println("------------------------------------------------")
        //Thread.sleep(500)
        var opOut = gpromStructureToMimirOperator(0, provGpromNode, null)
        //GProMWrapper.inst.gpromFreeMemContext(memctx)
        
        opOut = BestGuesser.bestGuessQuery(db, opOut)
        opOut = db.backend.specializeQuery(opOut)
        val sql = db.ra.convert(opOut).toString
        println(sql)
        println("-------v GProM Oper RW v---------")
        println(getQueryResults(sql))
        println("-------^ GProM Oper RW ^---------") 
        
        
        (opOut, Seq("MIMIR_ROWID"))
    }
  }
   
   def getQueryResults(query:String) : String =  {
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
}