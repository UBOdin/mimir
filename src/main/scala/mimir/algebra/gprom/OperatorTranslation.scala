package mimir.algebra.gprom

import org.gprom.jdbc.jna._

import com.sun.jna.Memory
import com.sun.jna.Native

import mimir.algebra._
import mimir.ctables.CTables
import mimir.sql.sqlite.VGTermFunctions
import mimir.provenance.Provenance
import mimir.views.ViewAnnotation
import mimir.ctables.CTPercolator
import mimir.serialization.Json
import com.sun.javafx.binding.SelectBinding.AsString

object ProjectionArgVisibility extends Enumeration {
   val Visible = Value("Visible")
   val Invisible = Value("Invisible") 
} 

object OperatorTranslation {
  var db: mimir.Database = null
  def gpromStructureToMimirOperator(depth : Int, gpromStruct: GProMStructure, gpromParentStruct: GProMStructure ) : Operator = {
    gpromStruct match {
      case aggregationOperator : GProMAggregationOperator => { 
        val (groupby, gbAnnotations) = getGroupByColumnsFromGProMAggragationOperator(aggregationOperator)
        val (aggregates, aggAnnotations) = getAggregatesFromGProMAggragationOperator(aggregationOperator)
        val source = gpromStructureToMimirOperator(depth+1,aggregationOperator.op.inputs, aggregationOperator)
        val visibleAggrs = groupby.map(_.name).union(aggregates.map(_.alias)).map(aggArgT => ProjectArg(aggArgT, Var(aggArgT)))
        val invisibleSchema = aggAnnotations ++ gbAnnotations
        val retOp = invisibleSchema match {
          case Seq() => Aggregate(groupby, aggregates, source)
          case _ if depth == 0 => Recover(new Project(visibleAggrs, Aggregate(groupby, aggregates, source)), invisibleSchema)
          case _ => Project(visibleAggrs, new Annotate(Aggregate(groupby, aggregates, source), invisibleSchema))
        }
        retOp
      }
      case attributeDef : GProMAttributeDef => { 
        throw new Exception("Translation Not Yet Implemented '"+attributeDef+"'") 
        }
      case constantRelationOperator : GProMConstRelOperator => { 
        //extract from prop hack
        /*val tableIntermSchema = extractTableSchemaGProMOperator(constantRelationOperator)
        val tableSchema = tableIntermSchema.map(tis => (tis.getAttrPrefix()+tis.attrName, tis.attrType))
        val firstRow = gpromListToScalaList(constantRelationOperator.values).map( cell => {
          translateGProMExpressionToMimirExpression(new GProMNode(cell.getPointer), tableIntermSchema).asInstanceOf[PrimitiveValue]
        }) 
        val data = firstRow +: extractHardTableHackFromConstRelPropHashmap(GProMWrapper.inst.castGProMNode(constantRelationOperator.op.properties).asInstanceOf[GProMHashMap], tableIntermSchema)
        HardTable(tableSchema, data)*/
        
        val tableIntermSchema = extractTableSchemaGProMOperator(constantRelationOperator)
        val tableSchema = tableIntermSchema.map(tis => (tis.getAttrPrefix()+tis.attrName, tis.attrType))
        val data = gpromListToScalaList(constantRelationOperator.values).map(row => gpromListToScalaList(row.asInstanceOf[GProMList]).map( cell => {
          translateGProMExpressionToMimirExpression(new GProMNode(cell.getPointer), tableIntermSchema).asInstanceOf[PrimitiveValue]
        }))
        val newOp =  HardTable(tableSchema, data)
        /*tableSchema.filter(_._1.equals("ROWID")) match {
          case Seq() => newOp
          case x => {
            val invisibleSchema = x.map(sche => ("MIMIR_ROWID", AnnotateArg(ViewAnnotation.PROVENANCE, "MIMIR_ROWID", TRowId(), Var(sche._1))))
            val visibleProjArgs = tableSchema.filterNot(_._1.equals("ROWID")).map(sche => ProjectArg(sche._1, Var(sche._1)))
            if(depth == 0){
              Recover(new Project(visibleProjArgs,newOp), invisibleSchema)
            }
            else Project(visibleProjArgs, new Annotate(newOp, invisibleSchema))
          }
        }*/
        newOp
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
        val newLhsRhs = makeBranchedRowids(Seq(Provenance.rowidColnameBase),Seq(Provenance.rowidColnameBase), lhs, rhs, false)
        new Select(condition, new Join(newLhsRhs._1, newLhsRhs._2))
        
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
        val orderSchema = getIntermediateSchemaFromGProMSchema(null,orderOperator.op.schema)
        //val sourceChild = gpromStructureToMimirOperator(depth+1, orderOperator.op, orderOperator)
        val gpOrderExprs = orderOperator.orderExprs
        val sortCols = translateGProMExpressionToMimirExpressionList(new GProMNode(gpOrderExprs.getPointer), orderSchema).map( expr => {
          SortColumn(expr, true)
        })
        val (visibleProjArgsTaint, invisibleSchemaTaint) = getTaintFromGProMQueryOperator(orderOperator.op)
        /*val (visibleProjArgsProv, invisibleSchemaProv) = getProvenanceFromGProMQueryOperator(orderOperator.op)
        val viz = visibleProjArgsTaint.toSet.intersect(visibleProjArgsProv.toSet)
        val visibleProjArgs = visibleProjArgsTaint.filter(elem => viz.contains(elem))
        val invisibleSchema = invisibleSchemaTaint ++ invisibleSchemaProv
        invisibleSchema match {
          case Seq() => Sort(sortCols, sourceChild)
          case _ if depth == 0 => Recover(new Project(visibleProjArgs, Sort(sortCols, sourceChild)), invisibleSchema)
          case _ => Project(visibleProjArgs, new Annotate(Sort(sortCols, sourceChild), invisibleSchema))
        }*/
        if(((orderOperator.op.provAttrs != null && orderOperator.op.provAttrs.length > 0) || invisibleSchemaTaint.length > 0) && depth == 0){
          val sourceChild = gpromStructureToMimirOperator(-1, orderOperator.op, orderOperator)
          sourceChild match {
            case Recover(proj@Project(visibleProjArgs, src), invisibleSchema) => new Recover(new Sort(sortCols, proj), invisibleSchema)
            case Recover(select@Select(cond, proj@Project(visibleProjArgs, src)), invisibleSchema) => new Recover(new Sort(sortCols, select), invisibleSchema)
            case x => throw new Exception("Translation error: Should Be Recover but got:'"+x+"'")
          }
        }
        else{
          val sourceChild = gpromStructureToMimirOperator(depth+1, orderOperator.op, orderOperator)
          new Sort(sortCols, sourceChild)
        }
      }
      case projectionOperator : GProMProjectionOperator => {
        val sourceChild = gpromStructureToMimirOperator(depth+1, projectionOperator.op, projectionOperator)
        val (visibleProjArgs, invisibleSchema) = getProjectionColumnsFromGProMProjectionOperator(projectionOperator)
        invisibleSchema match {
          case Seq() => Project(visibleProjArgs, sourceChild)
          case _ if depth == 0 => Recover(new Project(visibleProjArgs, sourceChild), invisibleSchema)
          case _ => Project(visibleProjArgs, new Annotate(sourceChild, invisibleSchema))
        }
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
          case GProM_JNA.GProMNodeTag.GProM_T_OrderOperator => gpromStructureToMimirOperator(depth+1, queryOperator.inputs, gpromParentStruct)
          case _ => gpromStructureToMimirOperator(depth+1, queryOperator.inputs, gpromParentStruct)
        }
      }
      case schema : GProMSchema => { 
        throw new Exception("Translation Not Yet Implemented '"+schema+"'") 
        }
      case selectionOperator : GProMSelectionOperator => { 
          val tableIntermSch = extractChildSchemaGProMOperator(selectionOperator)
          val condition = translateGProMExpressionToMimirExpression(new GProMNode(selectionOperator.cond.getPointer), tableIntermSch) 
          val (visibleProjArgsTaint, invisibleSchemaTaint) = getTaintFromGProMQueryOperator(selectionOperator.op)
          condition match {
            case Arithmetic(Arith.And,
                Comparison(Cmp.Gt, IntPrimitive(100000000), IntPrimitive(offset)), 
                Comparison(Cmp.Lt, IntPrimitive(-100000000), IntPrimitive(limitoff))) => {
              val limit = if((limitoff-offset) == -1) None else Some(limitoff-offset)    
              if(((selectionOperator.op.provAttrs != null && selectionOperator.op.provAttrs.length > 0) || invisibleSchemaTaint.length > 0)  && depth == 0){
                val sourceChild = gpromStructureToMimirOperator(-1, selectionOperator.op, selectionOperator)
                sourceChild match {
                  case Recover(proj@Project(visibleProjArgs, src), invisibleSchema) => new Recover(new Limit(offset, limit, proj), invisibleSchema)
                  case Recover(sort@Sort(_,Project(visibleProjArgs, src)), invisibleSchema) => new Recover(new Limit(offset, limit, sort), invisibleSchema)
                  case Recover(sort@Sort(_,Select(_,Project(visibleProjArgs, src))), invisibleSchema) => new Recover(new Limit(offset, limit, sort), invisibleSchema)
                  case x => throw new Exception("Translation error: Should Be Recover but got:'"+x+"'")
                }
              }
              else{
                val sourceChild = gpromStructureToMimirOperator(depth+1, selectionOperator.op, selectionOperator)
                new Limit(offset, limit, sourceChild)
              }     
            }
            case _ => {
			        if(((selectionOperator.op.provAttrs != null && selectionOperator.op.provAttrs.length > 0) || invisibleSchemaTaint.length > 0)  && depth == 0){
                val sourceChild = gpromStructureToMimirOperator(-1, selectionOperator.op, selectionOperator)
                sourceChild match {
                  case Recover(proj@Project(visibleProjArgs, src), invisibleSchema) => new Recover(new Select(condition, proj), invisibleSchema)
                  case Recover(sort@Sort(_,Project(visibleProjArgs, src)), invisibleSchema) => new Recover(new Select(condition, sort), invisibleSchema)
                  case Recover(sort@Sort(_,Select(_,Project(visibleProjArgs, src))), invisibleSchema) => new Recover(new Select(condition, sort), invisibleSchema)
                  case x => throw new Exception("Translation error: Should Be Recover but got:'"+x+"'")
                }
              }
              else{
                val sourceChild = gpromStructureToMimirOperator(depth+1, selectionOperator.op, selectionOperator)
                new Select(condition, sourceChild)
              }
            }
          }
        }
      case setOperator : GProMSetOperator => { 
        if(setOperator.setOpType == GProM_JNA.GProMSetOpType.GProM_SETOP_UNION){
          val lhs  = gpromStructureToMimirOperator(depth+1,setOperator.op.inputs.head, setOperator)
          val rhs = gpromStructureToMimirOperator(depth+1,setOperator.op.inputs.head.next, setOperator)
           val visibleAttrs = gpromListToScalaList(setOperator.op.schema.attrDefs).map {
            case attrDef:GProMAttributeDef => ProjectArg(attrDef.attrName, Var(attrDef.attrName))
          }
          val (taintvisibleAttrs, taint) = getTaintFromGProMQueryOperator(setOperator.op)
          val (provVisibleAttrs, prov) = getProvenanceFromGProMQueryOperator(setOperator.op)
          val lbranchBaseName = if(prov.length > 0) prov.head._1 else visibleAttrs.head.name//Provenance.rowidColnameBase
          val rbranchBaseName = if(prov.length > 1) prov.tail.head._1 else visibleAttrs.head.name//Provenance.rowidColnameBase
         
          val tableIntermSch = extractTableSchemaGProMOperator(setOperator) 
          val newLhsRhs = makeBranchedRowids(Seq(lbranchBaseName),Seq(rbranchBaseName), lhs, rhs, true)
          val newOp = Union(newLhsRhs._1, newLhsRhs._2)
          val newTaint = taint//.filter(col=>(!col._1.equals(lbranchBaseName) && !col._1.equals(rbranchBaseName)))
          val newViz = visibleAttrs//.filter(col=>(!col.name.equals(lbranchBaseName) && !col.name.equals(rbranchBaseName)))
          newTaint match {
            case Seq() => newOp 
            case _ if depth == 0 => Recover(new Project(newViz, newOp), newTaint++prov)
            case _ => Project(newViz, new Annotate(newOp, newTaint++prov))
          }
        }
        else  
          throw new Exception("Translation Not Yet Implemented '"+setOperator+"'") 
        }
      case tableAccessOperator : GProMTableAccessOperator => { 
          val tableIntermSchema = extractTableSchemaGProMOperator(tableAccessOperator)
          val tableSchemap = tableIntermSchema.map(tis => (tis.getAttrPrefix()+tis.attrName, tis.attrType))//getSchemaFromGProMQueryOperator(tableIntermSchema, tableAccessOperator.op)
          val (tableSchema, tableMeta) = tableSchemap.map(el => {
            val rowidPattern = ("((?:^ROWID$)|(?:^MIMIR_?ROWID(?:_?\\d+)?$))").r
            el._1 match {
              case rowidPattern(rowIDCol) => {
                (None, Some((rowIDCol, Var("ROWID"), TRowId())))
              }
              case _ => (Some(el), None)
            }
          }).unzip
          //val tableMeta = Seq[(String,Expression,Type)]() //tableSchema.map(tup => (tup._1,null,tup._2))
          //new Table(tableAccessOperator.tableName, tableIntermSchema(0).alias, tableSchema.flatten, tableMeta.flatten)
          val mimirRowIdName = Provenance.rowidColnameBase//s"${Provenance.rowidColnameBase}_$mimirRowIdCount"
          //mimirRowIdCount = mimirRowIdCount+1
          new Table(tableAccessOperator.tableName, tableIntermSchema(0).alias, tableSchema.flatten, Seq((mimirRowIdName, Var("ROWID"), TRowId())))
      }
      case updateOperator : GProMUpdateOperator => { 
        throw new Exception("Translation Not Yet Implemented '"+updateOperator+"'") 
        }
      case _ => { 
        null 
        }
    }
    
  }

  def makeBranchedRowids(lhsRowids:Seq[String],rhsRowids:Seq[String],lhs:Operator,rhs:Operator,branch:Boolean) : (Operator, Operator) = {
     val makeRowIDProjectArgs = 
          (rowids: Seq[String], offset: Integer, padLen: Integer) => {
            rowids.map(Var(_)).
                   padTo(padLen, RowIdPrimitive("-")).
                   zipWithIndex.map( { case (v, i) => 
                      val newName = Provenance.rowidColnameBase //+ "_" + (i+offset)
                      (newName, ProjectArg(newName, v))
                   }).
                   unzip
        }
           val (newLhsRowids, lhsIdProjections) = 
          makeRowIDProjectArgs(lhsRowids, 0, 0)
        val (newRhsRowids, rhsIdProjections) = 
          makeRowIDProjectArgs(rhsRowids, lhsRowids.size, 0)
        val lhsProjectArgs =
          lhs.columnNames/*.filter(col=>(!lhsRowids.contains(col) && !rhsRowids.contains(col)))*/.map(x => ProjectArg(x, Var(x))) ++ lhsIdProjections ++
          {if(branch)  List(ProjectArg(Provenance.rowidColnameBase+"_BRANCH", RowIdPrimitive("0"))) else List()}
        val rhsProjectArgs = 
          rhs.columnNames/*.filter(col=>(!lhsRowids.contains(col) && !rhsRowids.contains(col)))*/.map(x => ProjectArg(x, Var(x))) ++ rhsIdProjections ++
          {if(branch)  List(ProjectArg(Provenance.rowidColnameBase+"_BRANCH", RowIdPrimitive("1"))) else List()}
       
        //TODO: here we also need to handle other operator types with annotations and also handle Recover -Mike
        (lhs match {
          case Project(_, Annotate(_,annotationArgs)) => Project(lhsProjectArgs, Annotate(lhs, annotationArgs.filter(col=>(!lhsRowids.contains(col._1) && !rhsRowids.contains(col._1)))))
          case _ => Project(lhsProjectArgs, lhs)
        },
        rhs match {
          case Project(_, Annotate(_,annotationArgs)) => Project(rhsProjectArgs, Annotate(rhs, annotationArgs.filter(col=>(!lhsRowids.contains(col._1) && !rhsRowids.contains(col._1)))))
          case _ => Project(rhsProjectArgs, rhs)
        })
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
  
  val FN_UNCERT_WRAPPER = "UNCERT"
  
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
          case "ROWID" => Var(Provenance.rowidColnameBase)//RowIdVar()
          //case s if s.matches("_P_SIDE_.+") => Function("CAST", Seq(Var(attrMimirName),TypePrimitive(TInt())))
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
          case _ => throw new RuntimeException(s"Invalid Cast Expression: $castExpr")
        }
        Function("CAST", Seq(castArgs.head,fixedType)  )
      }
      case functionCall : GProMFunctionCall => {
        functionCall.functionname match {
          case "NOT" => {
            Not(translateGProMExpressionToMimirExpression(new GProMNode(functionCall.args.head.data.ptr_value), intermSchema))
          }
          case "sys_op_map_nonnull" => {
            val arg = translateGProMExpressionToMimirExpression(new GProMNode(functionCall.args.head.data.ptr_value), intermSchema)
            //arg
            Function("CAST", Seq(arg,TypePrimitive(TString())))
          }
          case "LEAST" => {
            Function("min", translateGProMExpressionToMimirExpressionList(new GProMNode(functionCall.args.getPointer), intermSchema))
          }
          case "GREATEST" => {
            Function("max", translateGProMExpressionToMimirExpressionList(new GProMNode(functionCall.args.getPointer), intermSchema))
          }
          case FN_UNCERT_WRAPPER => {
            translateGProMStructureToMimirExpression(functionCall.args.head, intermSchema)
          }
          case CTables.FN_TEMP_ENCODED => {
            val fargs = OperatorTranslation.gpromListToScalaList(functionCall.args).map(arg => {
              val mimirArg = translateGProMExpressionToMimirExpression(new GProMNode(arg.getPointer), intermSchema)
              mimirArg match {
                case Var("PROV_AGG_COLUMN__0") => Var("MIMIR_KR_HINT_COL_COLUMN_2") //TODO:  Super hack to fix.  somehow var arg in vg term converting incorrectly for projection over aggregate
                case x => x
              }
            })
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
            VGTerm(model.name, idx, vgtArgs, vgtHints)
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
      	//TODO: fix Translation of GProM OrderExpr -> Mimir Expression to include asc/desc (SortColumn is not expression so not 1 to 1)
      	translateGProMStructureToMimirExpression(GProMWrapper.inst.castGProMNode(new GProMNode(orderExpr.expr.getPointer)), intermSchema)
      }
      case rowNumExpr : GProMRowNumExpr => {
      	/*Var(Provenance.rowidColnameBase)*/RowIdVar()
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
      case keyValue : GProMKeyValue => {
        val key = translateGProMExpressionToMimirExpression(keyValue.key, intermSchema)
        val value = translateGProMExpressionToMimirExpression(keyValue.value, intermSchema)
        println(s"key: $key\nvalue:$value")
        value
      }
      case list:GProMList => {
        val mimirExprs = translateGProMExpressionToMimirExpressionList(new GProMNode(list.getPointer),intermSchema)
        mimirExprs(0)
      }
      case listCell : GProMListCell => { 
        val listCellDataGPStructure = new GProMNode(listCell.data.ptr_value)
        val cnvNode = GProMWrapper.inst.castGProMNode(listCellDataGPStructure)
        translateGProMStructureToMimirExpression(cnvNode, intermSchema)
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
      case rowNumberExpr : GProMRowNumExpr => {
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
  
  
  def translateGProMHashMapToScalaExpressionMap(hashMap: GProMHashMap, schema: Seq[MimirToGProMIntermediateSchemaInfo] ) : Map[String, Expression] = {
    if(hashMap == null)
      Map[String,Expression]()
    else{
      var scList = Seq[(String, Expression)]()
      var mapElem = hashMap.elem
      while(mapElem != null){
        val key = new GProMNode(mapElem.key)
        val value = new GProMNode(mapElem.data)
        if(key == null || value == null)
          println("WTF... there is some issue this should not be null")
        else{
          scList = scList :+ (translateGProMExpressionToMimirExpression( key,schema).toString(), translateGProMExpressionToMimirExpression( value,schema))
        }
        if(mapElem.hh.next == null)
          mapElem = null
        else{
          val elemnext = new GProMHashElem(mapElem.hh.next)
          mapElem = new GProMHashElem.ByReference()
          mapElem.key = elemnext.key
          mapElem.data = elemnext.data
          mapElem.hh = elemnext.hh
        }
          
      }
      scList.toMap
    }
  }
  
  def extractTaintFromGProMHashMap(hashMap: GProMHashMap, schema: Seq[MimirToGProMIntermediateSchemaInfo] ) : Seq[(GProMStructure, GProMStructure)] = {
    if(hashMap == null)
      Seq()
    else{
      var scList = Seq[(GProMStructure, GProMStructure)]()
      val taintMapNode = GProMWrapper.inst.gpromGetMapString(hashMap.getPointer, "UNCERT_MAPPING")
      if(taintMapNode != null){
        val taintMap = taintMapNode.asInstanceOf[GProMHashMap]
        var mapElem = taintMap.elem
        while(mapElem != null){
          val key = new GProMNode(mapElem.key)
          val value = new GProMNode(mapElem.data)
          if(key == null || value == null)
            println("WTF... there is some issue this should not be null")
          else{
            val (keyNode, valueNode) = 
            GProMWrapper.inst.castGProMNode(value) match {
              case keyValue : GProMKeyValue => (GProMWrapper.inst.castGProMNode(keyValue.key), GProMWrapper.inst.castGProMNode(keyValue.value))
              case _ => throw new Exception("WTF... there is some issue. this should be a key-value")
            }
            scList = scList :+ (keyNode, valueNode )
          }
          if(mapElem.hh.next == null)
            mapElem = null
          else{
            val elemnext = new GProMHashElem(mapElem.hh.next)
            mapElem = new GProMHashElem.ByReference()
            mapElem.key = elemnext.key
            mapElem.data = elemnext.data
            mapElem.hh = elemnext.hh
          }
            
        }
      }
      scList
    }
  }
  
  def extractProvMimirVarFromAggPropHashmap(hashMap: GProMHashMap, schema: Seq[MimirToGProMIntermediateSchemaInfo] ) : Seq[Expression] = {
    hashMap match {
      case null => Seq()
      case _ => GProMWrapper.inst.gpromGetMapString(hashMap.getPointer, "USER_PROV_ATTRS") match{
        case null => {
          println(s"$hashMap")
          Seq()
        }
        case node => gpromListToScalaList(node.asInstanceOf[GProMList])
                    .map(li => translateGProMStructureToMimirExpression(li, schema) match {
                      case StringPrimitive(provColName) => Var(provColName) //type not converting properly (ATTR Ref in gprom has string instead of int for COLLUMN_0) 
                      case x => x
                    } ) 
      }
    }
  }
  
  def extractProvGProMAttrFromPropHashmap(hashMap: GProMHashMap ) : Seq[GProMStructure] = {
    hashMap match {
      case null => Seq()
      case _ => GProMWrapper.inst.gpromGetMapString(hashMap.getPointer, "USER_PROV_ATTRS") match{
        case null => {
          println(s"$hashMap")
          Seq()
        }
        case node => gpromListToScalaList(node.asInstanceOf[GProMList])
      }
    }
  }
  
  def extractHardTableHackFromConstRelPropHashmap(hashMap: GProMHashMap, schema: Seq[MimirToGProMIntermediateSchemaInfo] ) : Seq[Seq[PrimitiveValue]] = {
    hashMap match {
      case null => Seq()
      case _ => GProMWrapper.inst.gpromGetMapString(hashMap.getPointer, "HARD_TABLE_DATA") match{
        case null => {
          println(s"$hashMap")
          Seq()
        }
        case node => gpromListToScalaList(node.asInstanceOf[GProMList])
                    .map(row => gpromListToScalaList(row.asInstanceOf[GProMList])
                        .map(cell => translateGProMStructureToMimirExpression(cell, schema).asInstanceOf[PrimitiveValue] ) )
      }
    }
  }
  
  def translateGProMHashMapToScalaMap(hashMap: GProMHashMap) : Map[GProMStructure, GProMStructure] = {
    if(hashMap == null)
      Map[GProMStructure,GProMStructure]()
    else{
      var scList = Seq[(GProMStructure, GProMStructure)]()
      var mapElem = hashMap.elem
      while(mapElem != null){
        val key = GProMWrapper.inst.castGProMNode(new GProMNode(mapElem.key))
        val value = GProMWrapper.inst.castGProMNode(new GProMNode(mapElem.data))
        if(key == null || value == null)
          println("WTF... there is some issue this should not be null")
        else{
          scList = scList :+ (key, value)
        }
        if(mapElem.hh.next == null)
          mapElem = null
        else{
          val elemnext = new GProMHashElem(mapElem.hh.next)
          mapElem = new GProMHashElem.ByReference()
          mapElem.key = elemnext.key
          mapElem.data = elemnext.data
          mapElem.hh = elemnext.hh
        }
      }
      scList.toMap
    }
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
          println("WTF... there is some issue this should not be null")
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
  
  trait Enum[A] {
    trait Value { self: A =>
      _values :+= this
    }
    private var _values = List.empty[A]
    def values = _values
  }
  
  def matchAnnotateArgNameToAnnotationType(name:String): ViewAnnotation.T = {
     val provenancePattern = ("PROV_.*").r
     val isDetPattern = "MIMIR_COL_DET_.*".r
     name match {
          case provenancePattern() => {
            ViewAnnotation.PROVENANCE
          }
          case isDetPattern() => {
            ViewAnnotation.TAINT
          }
          case x => {
            ViewAnnotation.OTHER 
          }
     }
  }
  
  sealed trait ProjectionArgVisibility extends ProjectionArgVisibility.Value
  object ProjectionArgVisibility extends Enum[ProjectionArgVisibility] {
    case object Visible extends ProjectionArgVisibility;  Visible 
    case object Invisible extends ProjectionArgVisibility; Invisible 
  }
  
  def getTaintFromGProMQueryOperator(op : GProMQueryOperator) : (Seq[ProjectArg], Seq[(String, AnnotateArg)]) = {
    val setSchema = getIntermediateSchemaFromGProMSchema(null,op.schema)
    val taintAndSrcAttrs = op.properties match {
      case null => Seq()
      case x => extractTaintFromGProMHashMap(GProMWrapper.inst.castGProMNode(op.properties).asInstanceOf[GProMHashMap], setSchema).map {
        case (srcAttrRef:GProMAttributeReference, taintAttrRef:GProMAttributeReference) => (taintAttrRef, srcAttrRef)
        case _ => throw new Exception("taint properties should be attribute refferences")
      }
    }
    taintAndSrcAttrs.map(taintAndSrc => {
      val mimirAnnoExpr = translateGProMExpressionToMimirExpression(new GProMNode(taintAndSrc._1.getPointer), setSchema)
      val mimirSrcExpr = translateGProMExpressionToMimirExpression(new GProMNode(taintAndSrc._2.getPointer), setSchema)
      (ProjectArg(taintAndSrc._2.name, mimirSrcExpr),
      (taintAndSrc._2.name, AnnotateArg(matchAnnotateArgNameToAnnotationType(taintAndSrc._1.name),taintAndSrc._1.name, getMimirTypeFromGProMDataType(taintAndSrc._1.attrType), mimirAnnoExpr)))
    }).unzip
  }
  
  def getProvenanceFromGProMQueryOperator(op : GProMQueryOperator) : (Seq[ProjectArg], Seq[(String, AnnotateArg)]) = {
    val setSchema = getIntermediateSchemaFromGProMSchema(null,op.schema)
    val attrDefs = gpromListToScalaList(op.schema.attrDefs)
    val provAttrs = gpromIntPointerListToScalaList(op.provAttrs)
    val provAttrNamesAndType = provAttrs match {
      case List() if(op.properties != null) => {
        extractProvGProMAttrFromPropHashmap(GProMWrapper.inst.castGProMNode(op.properties)
                                          .asInstanceOf[GProMHashMap]).flatMap {
          case const:GProMConstant => Some((translateGProMExpressionToMimirExpression(new GProMNode(const.getPointer), Seq()).asInstanceOf[PrimitiveValue].asString, GProM_JNA.GProMDataType.GProM_DT_STRING))
          case _ => None
        }
      }
      case x => x.map(pt => attrDefs(pt) match {
        case attrDef:GProMAttributeDef => (attrDef.attrName, attrDef.dataType)
      })
    } 
    provAttrNamesAndType.map(provAndType => {
        (ProjectArg(provAndType._1, Var(provAndType._1)),
        (provAndType._1, AnnotateArg(matchAnnotateArgNameToAnnotationType(provAndType._1),provAndType._1, getMimirTypeFromGProMDataType(provAndType._2), Var(provAndType._1))))
      }).unzip
  }
  
  def getProjectionColumnsFromGProMProjectionOperator(gpromProjOp : GProMProjectionOperator) : (Seq[ProjectArg], Seq[(String, AnnotateArg)]) = {
    val projExprs = gpromProjOp.projExprs;
    val projOpInputs =  gpromProjOp.op.inputs
    val childSchema = extractChildSchemaGProMOperator(gpromProjOp)
    val projSchema = getIntermediateSchemaFromGProMSchema(null,gpromProjOp.op.schema)
    val (taintAttrs, srcAttrs) =  (gpromProjOp.op.properties match {
      case null => Seq()
      case x => extractTaintFromGProMHashMap(GProMWrapper.inst.castGProMNode(gpromProjOp.op.properties).asInstanceOf[GProMHashMap], projSchema).map {
        case (srcAttrRef:GProMAttributeReference, taintAttrRef:GProMAttributeReference) => (taintAttrRef.attrPosition, srcAttrRef.name)
        case _ => throw new Exception("taint properties should be attribute refferences")
      }
    }).unzip

    val projExprsS = gpromListToScalaList(projExprs) 
    val provAndTaintAttrs = gpromIntPointerListToScalaList(gpromProjOp.op.provAttrs) ++ taintAttrs //++ projOpProps 
    val attrDefs = gpromListToScalaList(gpromProjOp.op.schema.attrDefs)
    val provAndTaintAttrNames = provAndTaintAttrs.map(pt => attrDefs(pt) match {
      case attrDef:GProMAttributeDef => attrDef.attrName
      case _ => throw new Exception("Attribute Def should Exist")
    })
    val (proj, anno) = projExprsS.zipWithIndex.map( expr => {
      val projExpr = new GProMNode(expr._1.getPointer)
      val i = expr._2 
      val mimirExpr = translateGProMExpressionToMimirExpression(projExpr, childSchema )
      val attrRefAttrPos = getGProMAttributeReferenceAttrPosition(projExpr, childSchema)
      var tablealias = ""
      if(childSchema.length > attrRefAttrPos)
        tablealias = childSchema(attrRefAttrPos).alias 
      if(!provAndTaintAttrs.contains(i)){
        (Some(ProjectArg(projSchema(i).attrProjectedName, mimirExpr)), None:Option[(String, AnnotateArg)])
      }
      else {
        (None:Option[ProjectArg], Some((provAndTaintAttrNames(provAndTaintAttrs.indexOf(i)), AnnotateArg(matchAnnotateArgNameToAnnotationType(projSchema(i).attrName),projSchema(i).attrProjectedName, projSchema(i).attrType, mimirExpr))))
      } 
    }).toSeq.unzip
    (proj.flatten, anno.flatten)
  }
  

  
  def getGroupByColumnsFromGProMAggragationOperator(gpromAggOp : GProMAggregationOperator) : (Seq[Var], Seq[(String, AnnotateArg)]) = {
    val gropByExprs = gpromAggOp.groupBy;
    gropByExprs match {
      case null => (Seq(), Seq())
      case x => {  
        val aggOpInputs =  gpromAggOp.op.inputs
        val aggSchema = getIntermediateSchemaFromGProMStructure(gpromAggOp)
        
        val projInputHead = new GProMNode(aggOpInputs.head.data.ptr_value)
        val arrgIntermSch = projInputHead.`type` match {
          case GProM_JNA.GProMNodeTag.GProM_T_ProjectionOperator => aggSchema
          case GProM_JNA.GProMNodeTag.GProM_T_TableAccessOperator => extractChildSchemaGProMOperator(gpromAggOp)
          case _ => aggSchema
        }
        
        val (taintAttrs, taintSrcAttrs) =  (gpromAggOp.op.properties match {
          case null => Seq()
          case x => extractTaintFromGProMHashMap(GProMWrapper.inst.castGProMNode(gpromAggOp.op.properties).asInstanceOf[GProMHashMap], aggSchema).map {
            case (srcAttrRef:GProMAttributeReference, taintAttrRef:GProMAttributeReference) => (taintAttrRef.attrPosition, srcAttrRef.name)
            case _ => throw new Exception("taint properties should be attribute refferences")
          }
        }).unzip
        
        val attrDefs = gpromListToScalaList(gpromAggOp.op.schema.attrDefs) 
       val groupByExprsS = gpromListToScalaList(gropByExprs)
       val (provAttrs, provSrcAttrs) = (gpromIntPointerListToScalaList(gpromAggOp.op.provAttrs),
                                    extractProvMimirVarFromAggPropHashmap( 
                                        GProMWrapper.inst.castGProMNode(gpromAggOp.op.properties)
                                          .asInstanceOf[GProMHashMap], aggSchema))
       
       val srcMap = provSrcAttrs.zipWithIndex.map(el => el._1 -> {
         try{
           val attrIdx = provAttrs.zipWithIndex.toMap.getOrElse(el._2, attrDefs.indexWhere(attr => {
             val attrName = attr.asInstanceOf[GProMAttributeDef].attrName  
             attrName.equalsIgnoreCase(el._1.toString()) || attrName.equalsIgnoreCase("PROV_AGG_" + el._1.toString().replaceAll("_", "__"))
           }))
           val attrDef = attrDefs(attrIdx).asInstanceOf[GProMAttributeDef]
           (ViewAnnotation.PROVENANCE, (attrDef.attrName, getMimirTypeFromGProMDataType(attrDef.dataType)))
         } catch {
             case t: Throwable => throw new Exception(s"Error: $t: \n${el}\n$provAttrs\n$attrDefs")
           }
         }).toMap ++ taintSrcAttrs.zipWithIndex.map(el => el._1 -> {
           val attrDef = attrDefs(taintAttrs(el._2)).asInstanceOf[GProMAttributeDef]
           (ViewAnnotation.TAINT, (attrDef.attrName, getMimirTypeFromGProMDataType(attrDef.dataType)))
         }).toMap

        val (gb, anno) = groupByExprsS.map(expr => {
          val groupByExpr = new GProMNode(expr.getPointer)
          val mimirExpr = translateGProMExpressionToMimirExpression(groupByExpr, arrgIntermSch )
          srcMap.get(mimirExpr) match {
            case Some((annoType, (attrName, attrType))) => (Some(mimirExpr.asInstanceOf[Var]), Some((attrName, AnnotateArg(annoType, attrName, attrType, mimirExpr))))
            case None => (Some(mimirExpr.asInstanceOf[Var]), None:Option[(String, AnnotateArg)]) 
          }    
        }).unzip
        (gb.flatten, anno.flatten)
      }
    }
  }
  
  def getAggregatesFromGProMAggragationOperator(gpromAggOp : GProMAggregationOperator) : (Seq[AggFunction],Seq[(String, AnnotateArg)])= {
    val aggrs = gpromAggOp.aggrs;
    aggrs match { 
      case null => (Seq(),Seq())
      case _ => {
        //val gropByExprs = gpromListToScalaList( gpromAggOp.groupBy)
        val aggOpInputs =  gpromAggOp.op.inputs
        //val childSchema = extractChildSchemaGProMOperator(gpromAggOp)
        val aggSchema = getIntermediateSchemaFromGProMStructure(gpromAggOp)
        
        val projInputHead = new GProMNode(aggOpInputs.head.data.ptr_value)
        val arrgIntermSch = projInputHead.`type` match {
          case GProM_JNA.GProMNodeTag.GProM_T_ProjectionOperator => aggSchema
          case GProM_JNA.GProMNodeTag.GProM_T_TableAccessOperator => extractChildSchemaGProMOperator(gpromAggOp)
          case _ => aggSchema
        }
     
         val gpAggSchema = gpromListToScalaList(gpromAggOp.op.schema.attrDefs).map{
           case attrDef:GProMAttributeDef => (attrDef.attrName, getMimirTypeFromGProMDataType(attrDef.dataType))  
           case x => throw new Exception("this should be an attr def " + x)
         }
        
         val (taintAttrs, srcAttrs) =  (gpromAggOp.op.properties match {
          case null => Seq()
          case x => {
            val taint = extractTaintFromGProMHashMap(GProMWrapper.inst.castGProMNode(gpromAggOp.op.properties).asInstanceOf[GProMHashMap], aggSchema)
            taint.map {
            case (srcAttrRef:GProMAttributeReference, taintAttrRef:GProMAttributeReference) => ((taintAttrRef.name, gpAggSchema(taintAttrRef.attrPosition-1)._1, taintAttrRef.attrPosition-1), srcAttrRef.name)
            case _ => throw new Exception("taint properties should be attribute refferences")
          }}
        }).unzip
            
        /*val gbAgg = gropByExprs.map { 
          case ar:GProMAttributeReference => AggFunction("FIRST", true, Seq(Var(ar.name)), ar.name)
        }*/
        val provAttrs = gpromIntPointerListToScalaList(gpromAggOp.op.provAttrs).map(i => (gpAggSchema(i)._1, i))
        val provAndTaintAttrs = provAttrs.map(provEl => aggSchema.indexWhere(el => el.attrName.equalsIgnoreCase(provEl._1))) ++ taintAttrs.map(taintEl => aggSchema.indexWhere(el => el.attrName.equalsIgnoreCase(taintEl._1)))
        val attrDefs = gpromListToScalaList(gpromAggOp.op.schema.attrDefs)
        val provAndTaintAttrNames = provAndTaintAttrs.map(pt => attrDefs(pt) match {
          case attrDef:GProMAttributeDef => attrDef.attrName
        })
        val aggrsS = gpromListToScalaList(aggrs)
        val (aggFuncs, anno) = aggrsS.zipWithIndex.map(expr => {
          val i = expr._2
          var distinct = false
          val aggr = new GProMFunctionCall(expr._1.getPointer)
          val mimirAggrFunc = translateGProMExpressionToMimirExpression(new GProMNode(expr._1.getPointer), arrgIntermSch) 
          val aggrArgs = aggr.args
          val mimirAggrArgs = if(aggrArgs != null){
            val aggrArgsS = gpromListToScalaList(aggrArgs)
            aggrArgsS.map( argExpr => {
              val aggrArg =  new GProMNode(argExpr.getPointer)
              val mimirExpr = translateGProMExpressionToMimirExpression(aggrArg, arrgIntermSch )
              mimirExpr match {
                case Function("DISTINCT", distinctArgs) => {
                  distinct = true
                  distinctArgs(0)  
                }
                case _ => mimirExpr 
              }
            })
          }
          else
            Seq[Expression]()
          if(!provAndTaintAttrs.contains(i)) {
            (Some(AggFunction(aggr.functionname, distinct, mimirAggrArgs, aggSchema(i).attrMimirName)), None:Option[(String, AnnotateArg)])    
          }
          else {
            (None:Option[AggFunction], Some((provAndTaintAttrNames(provAndTaintAttrs.indexOf(i)), AnnotateArg(matchAnnotateArgNameToAnnotationType(aggSchema(i).attrName), aggSchema(i).attrName, aggSchema(i).attrType, mimirAggrFunc))))
          }
        }).toSeq.unzip
        (aggFuncs.flatten, anno.flatten)
      }
    }
  }
  
  def extractProvAttributesFromGProMAggrOp(gpromAggOp : GProMAggregationOperator) = {
    val provAttrIdxs = gpromIntPointerListToScalaList(gpromAggOp.op.provAttrs) 
    provAttrIdxs match {
      case Seq() => {
        
      }
      case x => x
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
        case setOperator : GProMSetOperator => { 
            extractTableSchemaGProMOperator(setOperator.op.inputs)
          }
        case orderOperator : GProMOrderOperator => { 
            extractTableSchemaGProMOperator(orderOperator.op.inputs)
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
        case constRelOp : GProMConstRelOperator => {
          var listCell = constRelOp.op.schema.attrDefs.head
          (for(i <- 1 to constRelOp.op.schema.attrDefs.length ) yield {
            val attrDef = new GProMAttributeDef(listCell.data.ptr_value)
            listCell = listCell.next
            new MimirToGProMIntermediateSchemaInfo("", "", attrDef.attrName, attrDef.attrName, "", getMimirTypeFromGProMDataType(attrDef.dataType), i-1, 0)
          }).toSeq
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
        case setOperator : GProMSetOperator => { 
            extractChildSchemaGProMOperator(setOperator.op.inputs)
          }
        case orderOperator : GProMOrderOperator => { 
            extractChildSchemaGProMOperator(orderOperator.op.inputs)
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
        case constRelOp : GProMConstRelOperator => {
          extractTableSchemaGProMOperator(constRelOp)
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
        case orderOperator : GProMOrderOperator => { 
            getIntermediateSchemaFromGProMSchema(null,orderOperator.op.schema)
          }
        case joinOperator : GProMJoinOperator => { 
            extractTableSchemaGProMOperator(joinOperator.op.inputs)
          }
        case setOperator : GProMSetOperator => { 
            getIntermediateSchemaFromGProMSchema(null, setOperator.op.schema)
          }
        case constRelOp : GProMConstRelOperator => {
          getIntermediateSchemaFromGProMSchema(null, constRelOp.op.schema)
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
      /*attrProjName = attrProjName match {
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
      s"name: $name\nalias: $alias\nattrName: $attrName\nattrMimirName: $attrMimirName\nattrProjectedName: $attrProjectedName\nattrType: $attrType\nattrPosition: $attrPosition\nattrFromClausePosition: $attrFromClausePosition\n"
    }
  }
 /*( def mimirOperatorToGProMList(mimirOperator :  Operator) : GProMList.ByReference = {
    mimirOperatorToGProMList(mimirOperator, extractTableSchemaForGProM(mimirOperator))
  }*/
  //var toQoScm: GProMSchema.ByReference = null
  		
  def createDefaultGProMTablePropertiesMap(tableName:String, provCols:Seq[String] = Seq("ROWID")) : GProMHashMap = {
    val hasProvMapElemKey  = translateMimirPrimitiveExpressionToGProMConstant(StringPrimitive("HAS_PROVENANCE") ) 
    val hasProvMapElemValue = translateMimirPrimitiveExpressionToGProMConstant(BoolPrimitive(true) )  
    val provRelMapElemKey  = translateMimirPrimitiveExpressionToGProMConstant(StringPrimitive("PROVENANCE_REL_NAME") ) 
    val provRelMapElemValue = translateMimirPrimitiveExpressionToGProMConstant(StringPrimitive(tableName) ) 
    val provAttrMapElemKey  = translateMimirPrimitiveExpressionToGProMConstant(StringPrimitive("USER_PROV_ATTRS") ) 
    val provAttrMapElemValue = translateMimirExpressionsToGProMList(Seq(), provCols.map(StringPrimitive(_))) 
    provAttrMapElemValue.write()
    var gphashmap = GProMWrapper.inst.gpromAddToMap(null, hasProvMapElemKey.getPointer, hasProvMapElemValue.getPointer)
    gphashmap = GProMWrapper.inst.gpromAddToMap(gphashmap.getPointer, provRelMapElemKey.getPointer, provRelMapElemValue.getPointer)
    gphashmap = GProMWrapper.inst.gpromAddToMap(gphashmap.getPointer, provAttrMapElemKey.getPointer, provAttrMapElemValue.getPointer)
    gphashmap
  }
  
  def createDefaultGProMAggrPropertiesMap(relName:String, groupByCols:Seq[String]) : GProMHashMap = {
    val hasProvMapElemKey  = translateMimirPrimitiveExpressionToGProMConstant(StringPrimitive("HAS_PROVENANCE") ) 
    val hasProvMapElemValue = translateMimirPrimitiveExpressionToGProMConstant(BoolPrimitive(true) )  
    val provRelMapElemKey  = translateMimirPrimitiveExpressionToGProMConstant(StringPrimitive("PROVENANCE_REL_NAME") ) 
    val provRelMapElemValue = translateMimirPrimitiveExpressionToGProMConstant(StringPrimitive(relName) ) 
    val provAttrMapElemKey  = translateMimirPrimitiveExpressionToGProMConstant(StringPrimitive("USER_PROV_ATTRS") ) 
    val provAttrMapElemValue = translateMimirExpressionsToGProMList(Seq(), groupByCols.map(StringPrimitive(_))) 
    provAttrMapElemValue.write()
    var gphashmap = GProMWrapper.inst.gpromAddToMap(null, hasProvMapElemKey.getPointer, hasProvMapElemValue.getPointer)
    gphashmap = GProMWrapper.inst.gpromAddToMap(gphashmap.getPointer, provRelMapElemKey.getPointer, provRelMapElemValue.getPointer)
    gphashmap = GProMWrapper.inst.gpromAddToMap(gphashmap.getPointer, provAttrMapElemKey.getPointer, provAttrMapElemValue.getPointer)
    gphashmap
  }
  
  /*def createDefaultHardTableHackPropertiesMap(tableName:String, rows:Seq[Seq[GProMStructure]]) : GProMHashMap = {
    val hasProvMapElemKey  = translateMimirPrimitiveExpressionToGProMConstant(StringPrimitive("HAS_PROVENANCE") ) 
    val hasProvMapElemValue = translateMimirPrimitiveExpressionToGProMConstant(BoolPrimitive(true) )  
    val provRelMapElemKey  = translateMimirPrimitiveExpressionToGProMConstant(StringPrimitive("PROVENANCE_REL_NAME") ) 
    val provRelMapElemValue = translateMimirPrimitiveExpressionToGProMConstant(StringPrimitive(tableName) ) 
    val provAttrMapElemKey  = translateMimirPrimitiveExpressionToGProMConstant(StringPrimitive("USER_PROV_ATTRS") ) 
    val provAttrMapElemValue = translateMimirExpressionsToGProMList(Seq(), Seq(StringPrimitive("ROWID"))) 
    val htMapElemKey  = translateMimirPrimitiveExpressionToGProMConstant(StringPrimitive("HARD_TABLE_DATA") ) 
    val htMapElemValue = scalaListToGProMList(rows.map(el => scalaListToGProMList(el)))
    htMapElemValue.write()
    provAttrMapElemValue.write()
    
    var gphashmap = GProMWrapper.inst.gpromAddToMap(null, htMapElemKey.getPointer, htMapElemValue.getPointer)
    gphashmap = GProMWrapper.inst.gpromAddToMap(gphashmap.getPointer, htMapElemKey.getPointer, htMapElemValue.getPointer)
    gphashmap = GProMWrapper.inst.gpromAddToMap(gphashmap.getPointer, htMapElemKey.getPointer, htMapElemValue.getPointer)
    gphashmap = GProMWrapper.inst.gpromAddToMap(gphashmap.getPointer, provRelMapElemKey.getPointer, provRelMapElemValue.getPointer)
    gphashmap = GProMWrapper.inst.gpromAddToMap(gphashmap.getPointer, provAttrMapElemKey.getPointer, provAttrMapElemValue.getPointer)
    gphashmap
  }*/
  
  var toQoSchms : java.util.Vector[GProMSchema.ByReference] = new java.util.Vector[GProMSchema.ByReference]()
  def mimirOperatorToGProMList( mimirOperator :  Operator) : GProMList.ByReference = {
    synchronized { val list = new GProMList.ByReference()
    list.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
    list.length = 0;
    
    
    
    mimirOperator match {
			case Project(cols, src) => {
  			 val schTable = joinIntermSchemas(extractTableSchemaForGProM(mimirOperator), (src match {
            case Aggregate(_,_,_) => getSchemaForGProM(src)
            case _ => Seq[MimirToGProMIntermediateSchemaInfo]()
  			 }), 0, true)
  			 val schProj = getSchemaForGProM(mimirOperator)
  			 val schProjIn = getInSchemaForGProM(mimirOperator)
  			 
  			 //toQoSchms.add( translateMimirSchemaToGProMSchema("PROJECTION", schProj))
  			 //toQoSchms.lastElement().write()
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
        throw new Exception("Operator Translation not implemented '"+mimirOperator+"'")
      }
			case Recover(subj,invisScm) => {
        throw new Exception("Operator Translation not implemented '"+mimirOperator+"'")
      }
			
			case Aggregate(groupBy, agggregates, source) => {
			  val schTable = extractTableSchemaForGProM(mimirOperator)
        val schAggIn = getSchemaForGProM(source)
        val (userProvAttrs, userProvAggrs, newMimirOp) = groupBy match {
          case Seq() => {
            val newaggrs = Seq(AggFunction("SUM", false, Seq(IntPrimitive(1)), "USER_PROV")).union(agggregates)
            (Seq("USER_PROV"), newaggrs, Aggregate(groupBy, newaggrs, source))
          }
          case x => (x.map(_.name), agggregates, mimirOperator)
        }
			  val gpromAggrs = translateMimirAggregatesToGProMList(schTable.union(schAggIn), userProvAggrs)
			  val gpromGroupBy = translateGProMExpressionsToGProMList(translateMimirGroupByToGProMList(schTable.union(schAggIn), groupBy))
        val schAggr = getSchemaForGProM(newMimirOp)
  			val toQoScm = translateMimirSchemaToGProMSchema("AGG", schAggr)

			  
			  val gqoProps = createDefaultGProMAggrPropertiesMap("AGG", userProvAttrs)
			  val gqoInputs = mimirOperatorToGProMList(source)
  			val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_AggregationOperator, gqoInputs, toQoScm, null, null, new GProMNode.ByReference(gqoProps.getPointer))
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
			  val leftList = mimirOperatorToGProMList(lhs)
			  val rightList = mimirOperatorToGProMList(rhs)
			  val schTable = joinIntermSchemas(getSchemaForGProM(lhs), getSchemaForGProM(rhs), 1, false)
        val toQoScm = translateMimirSchemaToGProMSchema("JOIN", schTable)
			  val joinInputsList = joinGProMLists(leftList, rightList) 
        val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_JoinOperator, joinInputsList, toQoScm, null, null, null)
			  val gpcond = translateMimirExpressionToGProMCondition(condition, schTable) 
        val gpnbr = new GProMNode.ByReference(gpcond.getPointer)
			  val gpjoinop = new GProMJoinOperator.ByValue(gqo, GProM_JNA.GProMJoinType.GProM_JOIN_LEFT_OUTER, gpnbr)
			  setGProMQueryOperatorParentsList(leftList,gpjoinop)
			  setGProMQueryOperatorParentsList(rightList,gpjoinop)
			  list.head = createGProMListCell(gpjoinop) 
			  list.length += 1; 
			  list
			}
			case Join(lhs, rhs) => {
			  mimirOperatorToGProMList(Select(BoolPrimitive(true), mimirOperator))
			}
			case Union(lhs, rhs) => {
			  val leftList = mimirOperatorToGProMList(lhs)
			  val rightList = mimirOperatorToGProMList(rhs)
			  val schTable = joinIntermSchemas(getSchemaForGProM(lhs), getSchemaForGProM(rhs), 1, false)
        val toQoScm = translateMimirSchemaToGProMSchema("UNION", schTable)
			  val unionInputsList = joinGProMLists(leftList, rightList) 
        val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_SetOperator, unionInputsList, toQoScm, null, null, null)
			  val gpjoinop = new GProMSetOperator.ByValue(gqo, GProM_JNA.GProMSetOpType.GProM_SETOP_UNION)
			  setGProMQueryOperatorParentsList(leftList,gpjoinop)
			  setGProMQueryOperatorParentsList(rightList,gpjoinop)
			  list.head = createGProMListCell(gpjoinop) 
			  list.length += 1; 
			  list
			}
			case Limit(offset, limit, query) => {
			  //"TODO: handle limit translation....  but how?"
			  //println("TODO: handle limit translation....  but how?")
			  //mimirOperatorToGProMList(query)
			  
			  val cond = ExpressionUtils.makeAnd(Comparison(Cmp.Gt, IntPrimitive(100000000L), IntPrimitive(offset)), Comparison(Cmp.Lt, IntPrimitive(-100000000L), IntPrimitive(offset + limit.getOrElse(-1L))))
			  val schTable = getSchemaForGProM(mimirOperator)
        val toQoScm = translateMimirSchemaToGProMSchema("SELECT", schTable)
        val gqoInputs = mimirOperatorToGProMList(query)
  			val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_SelectionOperator, gqoInputs, toQoScm, null, null, null) 
        val gpcond = translateMimirExpressionToGProMCondition(cond, schTable) 
        val gpnbr = new GProMNode.ByReference(gpcond.getPointer)
			  val gpselop = new GProMSelectionOperator.ByValue(gqo, gpnbr )
			  setGProMQueryOperatorParentsList(gqoInputs,gpselop)
			  list.head = createGProMListCell(gpselop) 
			  list.length += 1; 
			  list
			}
			/*case Table("HARD_TABLE", "HARD_TABLE", sch, _) => {
			  var schTable = getSchemaForGProM(mimirOperator)
			  var hasRowID = false;
			  val toQoScm = translateMimirSchemaToGProMSchema("HARD_TABLE", schTable)//mimirOperator)
			  val gqoProps = createDefaultGProMTablePropertiesMap("HARD_TABLE")
			  val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_TableAccessOperator, null, toQoScm, null, null, new GProMNode.ByReference(gqoProps.getPointer))
			  val gpromTable = new GProMTableAccessOperator.ByValue(gqo,"HARD_TABLE",null)
			  list.head = createGProMListCell(gpromTable)
			  list.length += 1 
			  list
			}*/
			case Table(name, alias, sch, meta) => {
			  var schTable = getSchemaForGProM(mimirOperator)
			  var hasRowID = false;
			  val rowidPattern = ("((?:^ROWID$)|(?:^MIMIR_?ROWID(?:_?\\d+)?$))").r    
			  schTable = schTable.map(x => {
			     x.attrName match {
              case rowidPattern(rowIDCol) => {
                hasRowID = true
                new MimirToGProMIntermediateSchemaInfo(x.name, x.alias, RowIdVar().toString(), "ROWID", x.attrProjectedName, db.backend.rowIdType, x.attrPosition, x.attrFromClausePosition)
              }
              case _ => x
            }
			  })
			  if(!hasRowID)
			    schTable = joinIntermSchemas(schTable, Seq(new MimirToGProMIntermediateSchemaInfo(schTable(0).name, schTable(0).alias, RowIdVar().toString(), "ROWID", "", db.backend.rowIdType, 0, 0) ), 0, true)
			  val toQoScm = translateMimirSchemaToGProMSchema(alias, schTable)//mimirOperator)
			  val gqoProps = createDefaultGProMTablePropertiesMap(alias)
			  val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_TableAccessOperator, null, toQoScm, null, null, new GProMNode.ByReference(gqoProps.getPointer))
			  val gpromTable = new GProMTableAccessOperator.ByValue(gqo,name,null)
			  list.head = createGProMListCell(gpromTable)
			  list.length += 1 
			  list
			}
			case View(_, query, _) => {
			  mimirOperatorToGProMList(query)
			}
      case AdaptiveView(_, _, query, _) => {
        mimirOperatorToGProMList(query)
      }
      case HardTable(schema, data) => {
        //as union of inline projections
        /*val inlineProjs = data.zipWithIndex.map(row => 
          Project( schema.zip(row._1).map(cell => ProjectArg(cell._1._1, cell._2)) :+ ProjectArg("MIMIR_ROWID", RowIdPrimitive(s"${row._2}") ), Table("HARD_TABLE", "HARD_TABLE",schema,Seq())) )
        mimirOperatorToGProMList(inlineProjs.tail.foldLeft(inlineProjs.head:Operator)((init, elem) => Union(init, elem)))
        */
        
        //as json table
        /*var schTable = getSchemaForGProM(mimirOperator)
			  val toQoScm = translateMimirSchemaToGProMSchema("", schTable)
        val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_JsonTableOperator, null, toQoScm, null, null, null)
			  val columnList = translateMimirSchemaToJsonColInfoGProMList(schema)
        val gpjsontableop = new GProMJsonTableOperator.ByValue(gqo, columnList, Json.ofOperator(mimirOperator).toString(), null, schema.hashCode().toString, null)
			  list.head = createGProMListCell(gpjsontableop)
			  list.length += 1 
			  list*/
        
        //as union of constant rel ops
        /*var schTable = getSchemaForGProM(mimirOperator) 
        schTable = schTable :+ new MimirToGProMIntermediateSchemaInfo("", "", RowIdVar().toString(), "ROWID", "ROWID", db.backend.rowIdType, schTable.length, 0)
			  val toQoScm = translateMimirSchemaToGProMSchema("", schTable)
			  val gqoProps = createDefaultGProMTablePropertiesMap("HARD_TABLE")
        val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_ConstRelOperator, null, toQoScm, null, null, new GProMNode.ByReference(gqoProps.getPointer))
			  val gpconstrelop = new GProMConstRelOperator.ByValue(gqo, translateMimirExpressionsToGProMList(schTable, data.head:+RowIdPrimitive("0")))
        val resOp = data.tail.zipWithIndex.foldLeft(gpconstrelop:GProMStructure)((init, row) => {
          val toQoScm = translateMimirSchemaToGProMSchema("UNION", schTable)
          val gqoPropsin = createDefaultGProMTablePropertiesMap("HARD_TABLE")
          val gqoin = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_ConstRelOperator, null, toQoScm, null, null, new GProMNode.ByReference(gqoPropsin.getPointer))
			    val gpconstrelopun = new GProMConstRelOperator.ByValue(gqoin, translateMimirExpressionsToGProMList(schTable, row._1:+RowIdPrimitive((row._2+1).toString())))
          val unionInputsList = scalaListToGProMList(Seq(init, gpconstrelopun))
  			  val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_SetOperator, unionInputsList, toQoScm, null, null, null)
  			  val gpjoinop = new GProMSetOperator.ByValue(gqo, GProM_JNA.GProMSetOpType.GProM_SETOP_UNION)
  			  setGProMQueryOperatorParentsList(unionInputsList,gpjoinop)
  			  gpjoinop
        })
        //val gpconstrelop = new GProMConstRelOperator.ByValue(gqo, scalaListToGProMList(data.map(row => translateMimirExpressionsToGProMList(schTable, row))))
			  list.head = createGProMListCell(resOp)
			  list.length += 1 
			  list*/
        
        //as constant rel ops with prop hack
        /*var schTable = getSchemaForGProM(mimirOperator) 
        schTable = schTable :+ new MimirToGProMIntermediateSchemaInfo("", "", RowIdVar().toString(), "ROWID", "ROWID", db.backend.rowIdType, schTable.length, 0)
			  val toQoScm = translateMimirSchemaToGProMSchema("", schTable)
			  val gqoProps = createDefaultHardTableHackPropertiesMap("HARD_TABLE", data.tail.zipWithIndex.map(row => (row._1:+RowIdPrimitive((row._2+1).toString())).map(cell => translateMimirExpressionToGProMStructure(cell, schTable))))
        val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_ConstRelOperator, null, toQoScm, null, null, new GProMNode.ByReference(gqoProps.getPointer))
			  val gpconstrelop = new GProMConstRelOperator.ByValue(gqo, translateMimirExpressionsToGProMList(schTable, data.head:+RowIdPrimitive("0")))
        list.head = createGProMListCell(gpconstrelop)
			  list.length += 1 
			  list*/
			  
			  //as constant rel op properly
        var schTable = getSchemaForGProM(mimirOperator) 
        schTable = schTable :+ new MimirToGProMIntermediateSchemaInfo("", "", RowIdVar().toString(), "MIMIR_ROWID", "MIMIR_ROWID", db.backend.rowIdType, schTable.length, 0)
			  val toQoScm = translateMimirSchemaToGProMSchema("HARD_TABLE", schTable)
			  val gqoProps = createDefaultGProMTablePropertiesMap("HARD_TABLE", Seq("MIMIR_ROWID"))
        val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_ConstRelOperator, null, toQoScm, null, null, new GProMNode.ByReference(gqoProps.getPointer))
			  val gpconstrelop = new GProMConstRelOperator.ByValue(gqo, scalaListToGProMList(data.zipWithIndex.map(row => translateMimirExpressionsToGProMList(schTable, (row._1:+RowIdPrimitive((row._2).toString()))))))
        list.head = createGProMListCell(gpconstrelop)
			  list.length += 1 
			  list
        
      }
			case Sort(sortCols, src) => {
			  val schTable = getSchemaForGProM(mimirOperator)
        val toQoScm = translateMimirSchemaToGProMSchema("ORDER", schTable)
        val gqoInputs = mimirOperatorToGProMList(src)
  			val gqo = new GProMQueryOperator.ByValue(GProM_JNA.GProMNodeTag.GProM_T_OrderOperator, gqoInputs, toQoScm, null, null, null) 
        val gporderexprs = translateMimirSortColumnsToGProMList(schTable, sortCols) 
        val gporderop = new GProMOrderOperator.ByValue(gqo, gporderexprs )
			  setGProMQueryOperatorParentsList(gqoInputs,gporderop)
			  list.head = createGProMListCell(gporderop) 
			  list.length += 1; 
			  list
			}
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
                if(v.startsWith("MIMIR_ROWID_") || v.equals("MIMIR_ROWID")){
                   indexOfCol = schema.map(ct => ct.attrName).indexOf("MIMIR_ROWID")
                   if(indexOfCol < 0)
                     indexOfCol = schema.map(ct => ct.attrName).indexOf("ROWID")
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
      case VGTerm(name, idx, args, hints) => {
        val gpromExprList = translateMimirExpressionsToGProMList(schema, Seq(Function(CTables.FN_TEMP_ENCODED, Seq(StringPrimitive(name), IntPrimitive(idx)).union(args.union(hints)))))
        val gpromFunc = new GProMFunctionCall.ByValue(GProM_JNA.GProMNodeTag.GProM_T_FunctionCall, FN_UNCERT_WRAPPER, gpromExprList, 0)
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
      case d:DatePrimitive => {
        val dtStr = d.asString
        val strPtr = new Memory(dtStr.length()+1)
        strPtr.setString(0, dtStr);
        (GProM_JNA.GProMDataType.GProM_DT_STRING,strPtr,0)
      }
      case t:TimestampPrimitive => {
        val dtStr = t.asString
        val strPtr = new Memory(dtStr.length()+1)
        strPtr.setString(0, dtStr);
        (GProM_JNA.GProMDataType.GProM_DT_STRING,strPtr,0)
      }
      case i:IntervalPrimitive => {
        val dtStr = i.asString
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
      case VGTerm(name, idx, args, hints) => {
        Seq(StringPrimitive(name), IntPrimitive(idx)).union(args.union(hints)).map(param => translateMimirExpressionToStringForGProM(param, schema) ).mkString(s"${VGTermFunctions.bestGuessVGTermFn}(", ",", ")")
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
        val operSchema = try {
           db.typechecker.schemaOf(oper)   
        } catch {
          case t: Throwable => {
            println(s"problem getting schema: $oper: $t")
            throw t
          }
        }
            
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
        var aggrSchema = db.typechecker.schemaOf(oper)
        val aggrsAndGroupBy = {
          if(groupBy.length == 0 && !aggrSchema.contains(("USER_PROV", TInt()))){
            aggrSchema = Seq(("USER_PROV", TInt())).union(aggrSchema)
            Seq(AggFunction("SUM", false, Seq(IntPrimitive(1)), "USER_PROV")).union((agggregates.union(groupBy)))
          }
          else
            (agggregates.union(groupBy))
        }
        for(i <- 0 to (groupBy.length-1))
          aggrSchema = aggrSchema.tail.union(Seq(aggrSchema.head))
        val fullSchema = aggrsAndGroupBy.zip(aggrSchema).zipWithIndex
        fullSchema.map(aggr => {
          val attrNameInfo = (aggr._1._2._1/*"AGGR_"+aggr._2*/, aggr._1._2._1, "")//aggFunctionToGProMName(aggr._1._1, tableSchema(0))
          new MimirToGProMIntermediateSchemaInfo(tableSchema(0).name, tableSchema(0).alias, attrNameInfo._1, attrNameInfo._2, attrNameInfo._3, aggr._1._2._2, aggr._2, 0)
        })
        //getSchemaForGProM(source)
      }
      case Select(cond, source) => {
        getSchemaForGProM(source)
      }
      case Join(lhs, rhs) => {
        joinIntermSchemas(getSchemaForGProM(lhs), getSchemaForGProM(rhs), 0, true)
      }
      case Union(lhs, rhs) => {
        getSchemaForGProM(lhs)
      }
      case LeftOuterJoin(lhs, rhs, cond) => {
        joinIntermSchemas(getSchemaForGProM(lhs), getSchemaForGProM(rhs), 0, true)
      }
      case Limit(_,_,query) => {
        getSchemaForGProM(query)
      }
      case Sort(_,query) => {
        getSchemaForGProM(query)
      }
      case Table(name, alias, tgtSch, metadata) => {
         db.typechecker.schemaOf(oper).zipWithIndex.map(sch => new MimirToGProMIntermediateSchemaInfo(name, alias,  if(sch._1._1.startsWith(alias + "_")) sch._1._1.replaceFirst((alias + "_"), "") else sch._1._1, if(sch._1._1.startsWith(alias + "_")) sch._1._1 else alias + "_"+sch._1._1, "", sch._1._2, sch._2, 0))
      }
      case View(name, query, annotations) => {
        db.typechecker.schemaOf(oper).zipWithIndex.map(sch => new MimirToGProMIntermediateSchemaInfo(name, name, if(sch._1._1.startsWith(name + "_")) sch._1._1.replaceFirst((name + "_"), "") else sch._1._1, if(sch._1._1.startsWith(name + "_")) sch._1._1 else name + "_"+sch._1._1, "", sch._1._2, sch._2, 0))
      }
      case AdaptiveView(schema, name, query, annotations) => {
        db.adaptiveSchemas.get(schema) match {
          case Some(adaptiveSch) => { 
            val (mlens, config) = (adaptiveSch._1, adaptiveSch._2)
            db.query(
              mlens.attrCatalogFor(db, config).project("ATTR_NAME", "ATTR_TYPE") 
            ){ result =>
              if(result.hasNext){
                val sch = result.toList.map(row => (row(0).asString, Type.toSQLiteType(row(1).asInt))).zipWithIndex.map(sch => new MimirToGProMIntermediateSchemaInfo(name, name, if(sch._1._1.startsWith(name + "_")) sch._1._1.replaceFirst((name + "_"), "") else sch._1._1, if(sch._1._1.startsWith(name + "_")) sch._1._1 else name + "_"+sch._1._1, "", sch._1._2, sch._2, 0))
                if(annotations.contains(ViewAnnotation.PROVENANCE)){
                  var hasRowID = false;
          			  val rowidPattern = ("((?:^ROWID$)|(?:^MIMIR_?ROWID(?:_?\\d+)?$))").r    
          			  val schTable = sch.map(x => {
          			     x.attrName match {
                        case rowidPattern(rowIDCol) => {
                          hasRowID = true
                          new MimirToGProMIntermediateSchemaInfo(x.name, x.alias, RowIdVar().toString(), "ROWID", x.attrProjectedName, db.backend.rowIdType, x.attrPosition, x.attrFromClausePosition)
                        }
                        case _ => x
                      }
          			  })
          			  if(!hasRowID)
          			    joinIntermSchemas(schTable, Seq(new MimirToGProMIntermediateSchemaInfo(schTable(0).name, schTable(0).alias, RowIdVar().toString(), "ROWID", "", db.backend.rowIdType, 0, 0) ), 0, true)
                  else
                    schTable
                }
                else
                  sch
              }
              else{
                db.typechecker.schemaOf(oper).zipWithIndex.map(sch => new MimirToGProMIntermediateSchemaInfo(name, name, if(sch._1._1.startsWith(name + "_")) sch._1._1.replaceFirst((name + "_"), "") else sch._1._1, if(sch._1._1.startsWith(name + "_")) sch._1._1 else name + "_"+sch._1._1, "", sch._1._2, sch._2, 0))
              }
            }
          }
          case None => getSchemaForGProM(query)
        }
      }
      case HardTable(schema, data) => {
        val htName = s"HARD_TABLE_${schema.hashCode()}"
        schema.zipWithIndex.map(sch => new MimirToGProMIntermediateSchemaInfo(htName, htName, sch._1._1, sch._1._1, "", sch._1._2, sch._2, 0))
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
      case Union(lhs, rhs) => {
        extractTableSchemaForGProM(lhs)
      }
      case LeftOuterJoin(lhs, rhs, cond) => {
        joinIntermSchemas(extractTableSchemaForGProM(lhs), extractTableSchemaForGProM(rhs), 0, true)
      }
      case Limit(_,_,query) => {
        extractTableSchemaForGProM(query)
      }
      case Sort(_,query) => {
        extractTableSchemaForGProM(query)
      }
      case Table(name, alias, tgtSch, metadata) => {
        db.typechecker.schemaOf(oper).zipWithIndex.map(sch => new MimirToGProMIntermediateSchemaInfo(name, alias, if(sch._1._1.startsWith(alias + "_")) sch._1._1.replaceFirst((alias + "_"), "") else sch._1._1, if(sch._1._1.startsWith(alias + "_")) sch._1._1 else alias + "_"+sch._1._1, "", sch._1._2, sch._2, 0))
      }
      case View(name, query, annotations) => {
        db.typechecker.schemaOf(oper).zipWithIndex.map(sch => new MimirToGProMIntermediateSchemaInfo(name, name, if(sch._1._1.startsWith(name + "_")) sch._1._1.replaceFirst((name + "_"), "") else sch._1._1, if(sch._1._1.startsWith(name + "_")) sch._1._1 else name + "_"+sch._1._1, "", sch._1._2, sch._2, 0))
      }
      case AdaptiveView(schema, name, query, annotations) => {
        db.adaptiveSchemas.get(schema) match {
          case Some(adaptiveSch) => { 
            val (mlens, config) = (adaptiveSch._1, adaptiveSch._2)
            db.query(
              mlens.attrCatalogFor(db, config).project("ATTR_NAME", "ATTR_TYPE") 
            ){ result =>
             if(result.hasNext){
                val sch = result.toList.map(row => (row(0).asString, Type.toSQLiteType(row(1).asInt))).zipWithIndex.map(sch => new MimirToGProMIntermediateSchemaInfo(name, name, if(sch._1._1.startsWith(name + "_")) sch._1._1.replaceFirst((name + "_"), "") else sch._1._1, if(sch._1._1.startsWith(name + "_")) sch._1._1 else name + "_"+sch._1._1, "", sch._1._2, sch._2, 0))
                if(annotations.contains(ViewAnnotation.PROVENANCE)){
                  var hasRowID = false;
          			  val rowidPattern = ("((?:^ROWID$)|(?:^MIMIR_?ROWID(?:_?\\d+)?$))").r    
          			  val schTable = sch.map(x => {
          			     x.attrName match {
                        case rowidPattern(rowIDCol) => {
                          hasRowID = true
                          new MimirToGProMIntermediateSchemaInfo(x.name, x.alias, RowIdVar().toString(), "ROWID", x.attrProjectedName, db.backend.rowIdType, x.attrPosition, x.attrFromClausePosition)
                        }
                        case _ => x
                      }
          			  })
          			  if(!hasRowID)
          			    joinIntermSchemas(schTable, Seq(new MimirToGProMIntermediateSchemaInfo(schTable(0).name, schTable(0).alias, RowIdVar().toString(), "ROWID", "", db.backend.rowIdType, 0, 0) ), 0, true)
                  else
                    schTable
                }
                else
                  sch
              }
              else{
                db.typechecker.schemaOf(oper).zipWithIndex.map(sch => new MimirToGProMIntermediateSchemaInfo(name, name, if(sch._1._1.startsWith(name + "_")) sch._1._1.replaceFirst((name + "_"), "") else sch._1._1, if(sch._1._1.startsWith(name + "_")) sch._1._1 else name + "_"+sch._1._1, "", sch._1._2, sch._2, 0))
              }
            }
          }
          case None => extractTableSchemaForGProM(query)
        }
      }
      case HardTable(schema, data) => {
        val htName = s"HARD_TABLE_${schema.hashCode()}"
        schema.zipWithIndex.map(sch => new MimirToGProMIntermediateSchemaInfo(htName, htName, sch._1._1, sch._1._1, "", sch._1._2, sch._2, 0))
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
  
  def translateMimirOpSchemaToGProMSchema(schemaName: String, oper : Operator) : GProMSchema.ByReference = {
    var listCell : GProMListCell.ByReference = null
    var lastListCell : GProMListCell.ByReference = null
    var listTail : GProMListCell.ByReference = null
    var i = 0;
    
    val schema = db.typechecker.schemaOf(oper)
    for(schemaTup : (String, Type) <- schema.reverse){
      val attrName = schemaTup._1
          
      val attrDef = new GProMAttributeDef.ByValue(GProM_JNA.GProMNodeTag.GProM_T_AttributeDef, getGProMDataTypeFromMimirType(schemaTup._2 ), attrName);
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
  
  def translateMimirSortColumnsToGProMList(schema : Seq[MimirToGProMIntermediateSchemaInfo], cols : Seq[SortColumn]) : GProMList.ByReference = {
    var listCell : GProMListCell.ByReference = null
    var lastListCell : GProMListCell.ByReference = null
    var listTail : GProMListCell.ByReference = null
    var i = 0;
    for(sortCol : SortColumn <- cols.reverse){
      val exprGprom = translateMimirExpressionToGProMCondition(sortCol.expression, schema)
      val orderExpr = new GProMOrderExpr( GProM_JNA.GProMNodeTag.GProM_T_OrderExpr, new GProMNode.ByReference(exprGprom.getPointer), {if(sortCol.ascending) 0; else 1; }, 0 )
      val dataUnion = new GProMListCell.data_union.ByValue(orderExpr.getPointer)
      listCell = new GProMListCell.ByReference()
      if(i==0)
        listTail
      listCell.data = dataUnion
      listCell.next = lastListCell
      lastListCell = listCell;
      i+=1
    }
    val orderExprList = new GProMList.ByReference();
    orderExprList.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
    orderExprList.length = cols.length
    orderExprList.head = listCell
    orderExprList.tail = listTail
    orderExprList
  }
  
  def translateMimirGroupByToGProMList(schema : Seq[MimirToGProMIntermediateSchemaInfo], groupBy : Seq[Var]) : Seq[GProMStructure] = {
    if(groupBy.isEmpty)
      Seq()
    else{  
      groupBy.map(expr => translateMimirExpressionToGProMCondition(expr, schema))
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
        val gpromArg = {
          if(aggr.distinct)
            translateMimirExpressionToGProMCondition(Function("DISTINCT",Seq(arg)), schema)
          else
            translateMimirExpressionToGProMCondition(arg, schema)
        }
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
  
  def translateMimirSchemaToJsonColInfoGProMList(schema : Seq[(String, Type)]) : GProMList.ByReference = {
    val gpromExprs = new GProMList.ByReference()
    gpromExprs.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
    var j = 0;
    var exprListCell = gpromExprs.head
    schema.foreach(schel => {
      val nestedlist = new GProMList.ByReference()
      nestedlist.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
      nestedlist.length = 0;
      val gpromExpr = new GProMJsonColInfoItem.ByValue(GProM_JNA.GProMNodeTag.GProM_T_JsonColInfoItem, schel._1, "", getGProMDataTypeFromMimirType(schel._2).toString(), "", "", nestedlist, "")
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
  
  def scalaListToGProMList(exprs: Seq[GProMStructure]) : GProMList.ByReference = {
    val gpromExprs = new GProMList.ByReference()
    gpromExprs.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
    var j = 0;
    var exprListCell = gpromExprs.head
    exprs.foreach(gpromExpr => {
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
    gpromExprs.write() 
    gpromExprs 
  }
  
  def translateGProMExpressionsToGProMList(exprs: Seq[GProMStructure]) : GProMList.ByReference = {
    val gpromExprs = new GProMList.ByReference()
    gpromExprs.`type` = GProM_JNA.GProMNodeTag.GProM_T_List
    var j = 0;
    var exprListCell = gpromExprs.head
    exprs.foreach(gpromExpr => {
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
      case orderOperator : GProMOrderOperator => { 
          orderOperator.op.parents = createGProMQueryOperatorParentsList(parent)
          orderOperator.write()
        }
      case joinOperator : GProMJoinOperator => { 
          joinOperator.op.parents = createGProMQueryOperatorParentsList(parent)
          joinOperator.write()
        }
      case setOperator : GProMSetOperator => { 
          setOperator.op.parents = createGProMQueryOperatorParentsList(parent)
          setOperator.write()
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
       case jsonTable : GProMJsonTableOperator => { 
          jsonTable.op.parents = createGProMQueryOperatorParentsList(parent)
          jsonTable.write()
       }
       case constRel : GProMConstRelOperator => { 
          constRel.op.parents = createGProMQueryOperatorParentsList(parent)
          constRel.write()
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
  
  def createGProMListCell(intValue:Int) : GProMListCell.ByReference = {
    val listCell = new GProMListCell.ByReference()
    val dataUnion = new GProMListCell.data_union.ByValue(intValue)
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
    org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized{
      //db.backend.asInstanceOf[mimir.sql.GProMBackend].metadataLookupPlugin.setOper(oper)
        val memctx = GProMWrapper.inst.gpromCreateMemContext()
        //val memctxq = GProMWrapper.inst.createMemContextName("QUERY_CONTEXT")
        val gpromNode = mimirOperatorToGProMList(oper)
        gpromNode.write()
        val gpromNodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
        /*println("------------------------------------------------")
        println(gpromNodeStr)
        println("------------------------------------------------")*/
        val optimizedGpromNode = GProMWrapper.inst.optimizeOperatorModel(gpromNode.getPointer)
        val optNodeStr = GProMWrapper.inst.gpromNodeToString(optimizedGpromNode.getPointer())
        /*println("------------------------------------------------")
        println(oper)
        println("------------------------------------------------")
        println(optNodeStr)
        println("------------------------------------------------")*/
        //Thread.sleep(500)
        val opOut = gpromStructureToMimirOperator(0, optimizedGpromNode, null)
        GProMWrapper.inst.gpromFreeMemContext(memctx)
        opOut
    }
  }
   
   def compileProvenanceWithGProM(oper:Operator) : (Operator, Seq[String])  = {
    org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized{
      toQoSchms.clear()
      //db.backend.asInstanceOf[mimir.sql.GProMBackend].metadataLookupPlugin.setOper(oper)
        val memctx = GProMWrapper.inst.gpromCreateMemContext()
        //val memctxq = GProMWrapper.inst.createMemContextName("QUERY_CONTEXT")
        val gpromNode = mimirOperatorToGProMList(ProvenanceOf(oper))
        gpromNode.write()
        val gpNodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
        println("------------------------------------------------")
        println(gpNodeStr)
        println("------------------------------------------------")
        val provGpromNode = GProMWrapper.inst.provRewriteOperator(gpromNode.getPointer)
        //val optimizedGpromNode = GProMWrapper.inst.optimizeOperatorModel(provGpromNode.getPointer)
        val provNodeStr = GProMWrapper.inst.gpromNodeToString(provGpromNode.getPointer())
        println("------------------mimir-------------------------")
        println(oper)
        println("------------------gprom-------------------------")
        println(provNodeStr)
        println("------------------------------------------------")
        
        var opOut = gpromStructureToMimirOperator(0, provGpromNode, null)
        println("--------------mimir pre recover prov-----------------")
        println(opOut)
        println("-----------------------------------------------------")
        
        //GProMWrapper.inst.gpromFreeMemContext(memctxq)
        GProMWrapper.inst.gpromFreeMemContext(memctx)
        val (opRet, provCols) = provenanceColsFromRecover(recoverForRowId(opOut))
        println("--------------mimir post recover prov----------------")
        println(opRet)
        println("-----------------------------------------------------")
        //println(mimir.serialization.Json.ofOperator(opRet).toString)
        //release lock for JNA objs to gc
        (opRet, provCols)
    }
  }
   
  def compileTaintWithGProM(oper:Operator) : (Operator, Map[String,Expression], Expression)  = {
    //Thread.sleep(60000)
    org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized{
        toQoSchms.clear()
        //db.backend.asInstanceOf[mimir.sql.GProMBackend].metadataLookupPlugin.setOper(oper)
        val memctx = GProMWrapper.inst.gpromCreateMemContext()
        //val memctxq = GProMWrapper.inst.createMemContextName("QUERY_CONTEXT")
        val gpromNode = mimirOperatorToGProMList(oper)
        gpromNode.write()
        /*val gpNodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
        println("------------------------------------------------")
        println(gpNodeStr)
        println("------------------------------------------------")*/
        val provGpromNode = GProMWrapper.inst.taintRewriteOperator(gpromNode.head.data.ptr_value)
        //val optimizedGpromNode = GProMWrapper.inst.optimizeOperatorModel(provGpromNode.getPointer)
        val provNodeStr = GProMWrapper.inst.gpromNodeToString(provGpromNode.getPointer())
        println("------------------mimir-------------------------")
        println(oper)
        println("------------------gprom-------------------------")
        println(provNodeStr)
        println("------------------------------------------------")
        var opOut = gpromStructureToMimirOperator(0, provGpromNode, null)
        println("--------------mimir pre recover-----------------")
        println(opOut)
        println("------------------------------------------------")
        
        //GProMWrapper.inst.gpromFreeMemContext(memctxq)
        GProMWrapper.inst.gpromFreeMemContext(memctx)
        val (opRet, colTaint, rowTaint) = taintFromRecover(recoverForDet(opOut))
        /*println("--------------mimir post recover----------------")
        println(opRet)
        println("--------------------taint-----------------------")
        println(colTaint)
        println("------------------------------------------------")*/
        //println(mimir.serialization.Json.ofOperator(opRet).toString)
        //release lock for JNA objs to gc
        (opRet,  colTaint, rowTaint)
    }
  }
  
  /** combine gprom provenance and taint compilation into one method.
  * combining these steps saves a back and forth translation 
  * of the operator tree to/from gprom
  * 
  *  @param oper the operator to compile provenance and taint for
  *  @returns (the rewritten operator, provenance columns, column taint, row taint)
  */ 
  def compileProvenanceAndTaintWithGProM(oper:Operator) : (Operator, Seq[String], Map[String,Expression], Expression)  = {
    //Thread.sleep(60000)
    org.gprom.jdbc.jna.GProM_JNA.GC_LOCK.synchronized{
        toQoSchms.clear()
        //db.backend.asInstanceOf[mimir.sql.GProMBackend].metadataLookupPlugin.setOper(oper)
        val memctx = GProMWrapper.inst.gpromCreateMemContext()
        //val memctxq = GProMWrapper.inst.createMemContextName("QUERY_CONTEXT")
        val gpromNode = mimirOperatorToGProMList(oper)
        gpromNode.write()
        val gpNodeStr = GProMWrapper.inst.gpromNodeToString(gpromNode.getPointer())
        println("------------------------------------------------")
        println(gpNodeStr)
        println("------------------------------------------------")
       
        //rewrite for provenance with gprom 
        val provGpromNode = GProMWrapper.inst.provRewriteOperator(gpromNode.getPointer)
        //val optimizedGpromNode = GProMWrapper.inst.optimizeOperatorModel(provGpromNode.getPointer)
        val provNodeStr = GProMWrapper.inst.gpromNodeToString(provGpromNode.getPointer())
        println("------------------mimir-------------------------")
        println(oper)
        println("------------------gprom-------------------------")
        println(provNodeStr)
        println("------------------------------------------------")
        
        //rewrite for taint
        val taintGpromNode = GProMWrapper.inst.taintRewriteOperator(provGpromNode.asInstanceOf[GProMList].head.data.ptr_value)
        //val optimizedGpromNode = GProMWrapper.inst.optimizeOperatorModel(provGpromNode.getPointer)
        val taintNodeStr = GProMWrapper.inst.gpromNodeToString(provGpromNode.getPointer())
        println("------------------gprom-------------------------")
        println(taintNodeStr)
        println("------------------------------------------------")
        var opOut = gpromStructureToMimirOperator(0, taintGpromNode, null)
        println("--------------mimir pre recover-----------------")
        println(opOut)
        println("------------------------------------------------")
        
        //release lock for JNA objs to gc
        //GProMWrapper.inst.gpromFreeMemContext(memctxq)
        GProMWrapper.inst.gpromFreeMemContext(memctx)
        val (opProv, provCols) = provenanceColsFromRecover(recoverForRowId(opOut))
        println("--------------mimir post recover prov----------------")
        println(opProv)
        println("-----------------------------------------------------")
        //println(mimir.serialization.Json.ofOperator(opRet).toString)
        val (opRet, colTaint, rowTaint) = taintFromRecover(recoverForDet(opProv))
        println("--------------mimir post recover taint----------------")
        println(opRet)
        println("--------------------taint-----------------------------")
        println(colTaint)
        println("------------------------------------------------------")
        //println(mimir.serialization.Json.ofOperator(opRet).toString)
        //release lock for JNA objs to gc
        (opRet, provCols, colTaint, rowTaint)
    }
  }
   
  def recoverForRowId(oper:Operator) : Operator = {
    oper match {
      case Recover(subj,invisScm) => {
        Recover(subj, invisScm.filter(_._2.annotationType == ViewAnnotation.PROVENANCE))
      }
      case x => throw new Exception("Recover Op required, not: "+x.toString())
    }
  }
  
  def recoverForDet(oper:Operator) : Operator = {
    oper match {
      case Recover(subj,invisScm) => {
        Recover(subj, invisScm.filter(_._2.annotationType == ViewAnnotation.TAINT))
      }
      case x => throw new Exception("Recover Op required, not: "+x.toString())
    }
  }
  
  def recoverIgnoreInvis(oper:Operator) : Operator = {
    oper match {
      case Recover(subj,invisScm) => {
        Recover(subj, Seq())
      }
      case x => throw new Exception("Recover Op required, not: "+x.toString())
    }
  }
  
  def provenanceColsFromRecover(oper:Operator) : (Operator, Seq[String]) = {
    oper match {
     case Recover(subj,invisScm) => {
       (annotationsAndRecoveryToProjections(oper), invisScm.map(ise => ise._2.name))
     }
     case x => throw new Exception("Recover Op required, not: "+x.toString())
    }
  }
  
  def replaceRowIdVars(expr: Expression, rowIdCol :String): Expression = {
    expr match {
      case RowIdVar() => Var(rowIdCol)
      case _ => expr.rebuild(expr.children.map(replaceRowIdVars(_,rowIdCol)))
    }
  }
  
  def recoverProject(invisScm: Seq[AnnotateArg], cols:Seq[ProjectArg], recoveredOp: Operator) : Operator = {
     recoveredOp match {
      case Project(ncols, nsrc) => {
        //val schMap = nsrc.schema.toMap
        /*val srcColsMap = ncols.map(srcCol => (srcCol.name, srcCol)).toMap
        val noRemAnno = invisScm.map(ise => (ise.name, ProjectArg(ise.name, ise.expr))).toMap
        val rowIdCol = invisScm.filter(_.annotationType == ViewAnnotation.PROVENANCE)(0).name
        val newAnno = cols.map(col => {
          col.expression match {
            case Var(v) => {
              ProjectArg(col.name, replaceRowIdVars(srcColsMap(v).expression, rowIdCol))
            }
            case x => col
          }*/
        //}).union(noRemAnno/*.filter( p => schMap.contains(p._1))*/.toSeq.map(f => f._2))
        //Project(newAnno, nsrc)
          
        val rowIdCol = invisScm.filter(_.annotationType == ViewAnnotation.PROVENANCE)(0).name
        val combinedAnno = ncols.union(invisScm.map(ise =>  ProjectArg(ise.name, ise.expr)))
        val newAnno = combinedAnno.map(projArg => {
          ProjectArg(projArg.name, replaceRowIdVars(projArg.expression, rowIdCol))
        })
        Project(newAnno, nsrc)
        
      }
      /*case Select(cond, proj@Project(ncols, src)) => {
         Select(cond,recoverProject(invisScm, cols, proj))
      }
      case Select(cond, ssel@Select(scond,sop)) => {
        Select(cond,recoverProject(invisScm, cols, ssel))
      }*/
      case x => throw new Exception("Recover Op needs project, not: "+x.toString())
    }
  }
  
  def rejigSortCols(sortCols:Seq[SortColumn], projArgs:Seq[ProjectArg]) : Seq[SortColumn] = {
    val replacements = projArgs.map(projArg => (Var(projArg.name) -> projArg.expression)).toMap
    def recurReplace(expr:Expression) : Expression = expr match {
      case ref@Var(col) => replacements.getOrElse(ref, ref) 
      case x => x.recur(recurReplace)
    }
    sortCols.map(sortCol => SortColumn(sortCol.expression.recur(recurReplace), sortCol.ascending))
  }
  
  def rejigExpression(rexpr:Expression, projArgs:Seq[ProjectArg]) : Expression = {
    val replacements = projArgs.map(projArg => (Var(projArg.name) -> projArg.expression)).toMap
    def recurReplace(expr:Expression) : Expression = expr match {
      case ref@Var(col) => replacements.getOrElse(ref, ref) 
      case x => x.recur(recurReplace)
    }
    rexpr.recur(recurReplace)
  }
  
  def annotationsAndRecoveryToProjections(oper:Operator) : Operator = {
    oper match {
      case Recover(subj,invisScm) => {
        subj match {
          case Project(cols, src) => {
            val recoveredOp = annotationsAndRecoveryToProjections(src)
            recoverProject(invisScm.map(_._2), cols, Project(cols,recoveredOp))
          }  
          case Select(cond, tsrc) => {
            val (src, cols) = tsrc match {
              case Project(cols,src) => (src,cols)
              case Limit(o,l,Project(cols,psrc)) => (Limit(o,l,psrc), cols)
              case Limit(o,l,Sort(sc,Project(cols,psrc))) => (Limit(o,l,Sort(rejigSortCols(sc,cols),psrc)),cols)
              case Sort(sc,Project(cols,psrc)) => (Sort(rejigSortCols(sc,cols),psrc), cols)
              case Sort(sc,Limit(o,l,Project(cols,psrc))) => (Sort(rejigSortCols(sc,cols),Limit(o,l,psrc)),cols)
            }
            val rowIdCol = invisScm.filter(_._2.annotationType == ViewAnnotation.PROVENANCE)(0)._2.name
            val recoveredOp = annotationsAndRecoveryToProjections(src)
            Select(replaceRowIdVars(cond, rowIdCol),recoverProject(invisScm.map(_._2), cols, Project(cols,recoveredOp)))
          }
          case Sort(sortCols, tsrc) => {
            val (src, cols) = tsrc match {
              case Project(cols,src) => (src,cols)
              case Select(c,Project(cols,psrc)) => (Select(rejigExpression(c, cols),psrc), cols)
              case Select(c,Limit(o,l,Project(cols,psrc))) => (Select(rejigExpression(c, cols),Limit(o,l,psrc)),cols)
              case Limit(o,l,Project(cols,psrc)) => (Limit(o,l,psrc), cols)
              case Limit(o,l,Select(c,Project(cols,psrc))) => (Limit(o,l,Select(rejigExpression(c, cols),psrc)),cols)
            }
            val recoveredOp = annotationsAndRecoveryToProjections(src)
            Sort(sortCols, recoverProject(invisScm.map(_._2), cols, Project(cols,recoveredOp)))
          }
          case Limit(offset, limit, tsrc) => {
            val (src, cols) = tsrc match {
              case Project(cols,src) => (src,cols)
              case Select(c,Project(cols,psrc)) => (Select(rejigExpression(c, cols),psrc), cols)
              case Select(c,Sort(sc,Project(cols,psrc))) => (Select(rejigExpression(c, cols),Sort(rejigSortCols(sc,cols),psrc)),cols)
              case Sort(sc,Project(cols,psrc)) => (Sort(rejigSortCols(sc,cols),psrc), cols)
              case Sort(sc,Select(c,Project(cols,psrc))) => (Sort(rejigSortCols(sc,cols),Select(rejigExpression(c, cols),psrc)),cols)
            }
            val recoveredOp = annotationsAndRecoveryToProjections(src)
            Limit(offset, limit, recoverProject(invisScm.map(_._2), cols, Project(cols,recoveredOp)))
          }
          case x => throw new Exception("Recover Op needs project child, not: "+x.getClass.toString())
        }
      }
      case Project(cols, Annotate( Aggregate(groupBy, agggregates, source),invisScm)) => {
        val newagggregates = agggregates ++ invisScm.flatMap(isel => isel._2.expr match {
            case Function(name, Seq(Function("DISTINCT", args))) => Some(AggFunction(name, true, args, isel._1))
            case Function(name, args) => Some(AggFunction(name, false, args, isel._1))
            case v@Var(col) if(groupBy.contains(v)) => None//Some(AggFunction("FIRST", true, Seq(v), isel._1))
            case x => throw new Exception(s"Should be functioncall or var: was: ${x.getClass}:$x : gb: $groupBy")
          }) 
          val repAnno = invisScm.map(ise => ProjectArg(ise._2.name, ise._2.expr match {
            case v@Var(name) => v 
            case Function(_,_) => Var(ise._1)
          }))
          Project(cols ++ repAnno, Aggregate(groupBy, newagggregates, annotationsAndRecoveryToProjections(source)))
       }
      case Project(cols, src) => {
        src match {
         case Annotate(subj,invisScm) => {
            val repAnno = invisScm.map(_._2).map(ise => (ise.name, ProjectArg(ise.name, ise.expr))).toMap
            val provCols = invisScm.map(_._2).filter(_.annotationType == ViewAnnotation.PROVENANCE)
            val rowIdCol = provCols match {
              case Seq() => None
              case x => Some(x(0).name)
            }
            val colSet = cols.map(col => col.name).toSet
            val newAnno = invisScm.map(_._2).flatMap(ise => {
              if(colSet.contains(ise.name)){
                None
              }
              else
                subj match {
                  case Table(_,_,_,_) => Some(ProjectArg(ise.name, replaceRowIdVars(ise.expr,"MIMIR_ROWID"))  )
                  case _ => Some(ProjectArg(ise.name, ise.expr))
                }
            })
            
            val annotatedOp = Project(cols.map(col => {
              val projArg = {
                if(repAnno.contains(col.name)){
                  repAnno(col.name)
                }
                else{
                  col
                }
              }
              ProjectArg(projArg.name, rowIdCol match { 
                case Some(ridcol) => replaceRowIdVars(projArg.expression,ridcol)
                case None => projArg.expression  
              })  
            }).union(newAnno), annotationsAndRecoveryToProjections(subj) )
            annotatedOp
            
          }
          case x => {
            /*val newCols = cols.map(projArg => {
              ProjectArg(projArg.name, replaceRowIdVars(projArg.expression, "MIMIR_ROWID"))
            })*/
            Project(cols, annotationsAndRecoveryToProjections(src))
          }
        }
      }
      /*case Aggregate(groupBy, agggregates, source) => {
        //val provSrc  = compileProvenanceWithGProM(source)._1
        //Aggregate(groupBy, agggregates, annotationsAndRecoveryToProjections(provSrc)) 
        Aggregate(groupBy, agggregates, annotationsAndRecoveryToProjections(source))
      }*/
      case Select(cond, source) => {
         Select(cond,annotationsAndRecoveryToProjections(source))
      }
      case Join(lhs, rhs) => {
        Join(annotationsAndRecoveryToProjections(lhs), annotationsAndRecoveryToProjections(rhs))
      }
      case LeftOuterJoin(lhs, rhs, cond) => {
        LeftOuterJoin(annotationsAndRecoveryToProjections(lhs), annotationsAndRecoveryToProjections(rhs), cond)
      }
      case Limit(off,lim,query) => {
        Limit(off,lim,annotationsAndRecoveryToProjections(query))
      }
      case Union(lhs, rhs) => {
        Union(annotationsAndRecoveryToProjections(lhs), annotationsAndRecoveryToProjections(rhs))
      }
      case Sort(sortCols, source) => {
         Sort(sortCols,annotationsAndRecoveryToProjections(source))
      }
      case x => {
        //println("Op ---------------------------------")
        //println(x)
        x
      }
    }
  }
  
  def taintFromRecover(oper:Operator) : (Operator, Map[String,Expression], Expression) = {
    oper match {
     case Recover(subj,invisScm) => {
       (generalAnnotationsAndRecoveryToProjections(oper), invisScm.filter(!_._2.name.equals("MIMIR_COL_DET_R")).map(ise => (ise._1.replaceAll("MIMIR_COL_DET_", ""), Var(ise._2.name))).toMap, invisScm.filter(_._2.name.equals("MIMIR_COL_DET_R")).head._2.expr)
     }
     case x => throw new Exception("Recover Op required, not: "+x.toString())
    }
  }
  
  def generalRecoverProject(invisScm: Seq[AnnotateArg], cols:Seq[ProjectArg], recoveredOp: Operator) : Operator = {
     recoveredOp match {
      case Project(ncols, nsrc) => {
        val combinedAnno = ncols.union(invisScm.map(ise =>  ProjectArg(ise.name, ise.expr)))
        val newAnno = combinedAnno.map(projArg => {
          ProjectArg(projArg.name, projArg.expression)
        })
        Project(newAnno, nsrc)
      }
      case x => throw new Exception("Recover Op needs project, not: "+x.toString())
    }
  }
  
  def generalAnnotationsAndRecoveryToProjections(oper:Operator) : Operator = {
    oper match {
      case Recover(subj,invisScm) => {
        subj match {
          case Project(cols, src) => {
            val recoveredOp = generalAnnotationsAndRecoveryToProjections(src)
            generalRecoverProject(invisScm.map(_._2), cols, Project(cols,recoveredOp))
          }  
          case Select(cond, tsrc) => {
            val (src, cols) = tsrc match {
              case Project(cols,src) => (src,cols)
              case Limit(o,l,Project(cols,psrc)) => (Limit(o,l,psrc), cols)
              case Limit(o,l,Sort(sc,Project(cols,psrc))) => (Limit(o,l,Sort(rejigSortCols(sc,cols),psrc)),cols)
              case Sort(sc,Project(cols,psrc)) => (Sort(rejigSortCols(sc,cols),psrc), cols)
              case Sort(sc,Limit(o,l,Project(cols,psrc))) => (Sort(rejigSortCols(sc,cols),Limit(o,l,psrc)),cols)
            }
            val recoveredOp = generalAnnotationsAndRecoveryToProjections(src)
            Select(cond,generalRecoverProject(invisScm.map(_._2), cols, Project(cols,recoveredOp)))
          }
          case Sort(sortCols, tsrc) => {
            val (src, cols) = tsrc match {
              case Project(cols,src) => (src,cols)
              case Select(c,Project(cols,psrc)) => (Select(rejigExpression(c, cols),psrc), cols)
              case Select(c,Limit(o,l,Project(cols,psrc))) => (Select(rejigExpression(c, cols),Limit(o,l,psrc)),cols)
              case Limit(o,l,Project(cols,psrc)) => (Limit(o,l,psrc), cols)
              case Limit(o,l,Select(c,Project(cols,psrc))) => (Limit(o,l,Select(rejigExpression(c, cols),psrc)),cols)
            }
            val recoveredOp = generalAnnotationsAndRecoveryToProjections(src)
            Sort(sortCols, generalRecoverProject(invisScm.map(_._2), cols, Project(cols,recoveredOp)))
          }
          case Limit(offset, limit, tsrc) => {
            val (src, cols) = tsrc match {
              case Project(cols,src) => (src,cols)
              case Select(c,Project(cols,psrc)) => (Select(rejigExpression(c, cols),psrc), cols)
              case Select(c,Sort(sc,Project(cols,psrc))) => (Select(rejigExpression(c, cols),Sort(rejigSortCols(sc,cols),psrc)),cols)
              case Sort(sc,Project(cols,psrc)) => (Sort(rejigSortCols(sc,cols),psrc), cols)
              case Sort(sc,Select(c,Project(cols,psrc))) => (Sort(rejigSortCols(sc,cols),Select(rejigExpression(c, cols),psrc)),cols)
            }
            val recoveredOp = generalAnnotationsAndRecoveryToProjections(src)
            Limit(offset, limit, generalRecoverProject(invisScm.map(_._2), cols, Project(cols,recoveredOp)))
          }
          case x => throw new Exception("Recover Op needs project child, not: "+x.getClass.toString())
        }
      }
      case Project(cols, Annotate( Aggregate(groupBy, agggregates, source),invisScm)) => {
          val newagggregates = agggregates ++ invisScm.flatMap(isel => isel._2.expr match {
            case Function(name, Seq(Function("DISTINCT", args))) => Some(AggFunction(name, true, args, isel._1))
            case Function(name, args) => Some(AggFunction(name, false, args, isel._1))
            case v@Var(col) if(groupBy.contains(v)) => None//Some(AggFunction("FIRST", true, Seq(v), isel._1))
            case x => throw new Exception(s"Should be functioncall or var: was: ${x.getClass}:$x : gb: $groupBy")
          }) 
          val repAnno = invisScm.map(ise => ProjectArg(ise._2.name, Var(ise._1)))
          Project(cols++repAnno, Aggregate(groupBy, newagggregates, generalAnnotationsAndRecoveryToProjections(source)))
       }
      case Project(cols, src@Annotate(subj,invisScm)) => {
            val repAnno = invisScm.map(_._2).map(ise => (ise.name, ProjectArg(ise.name, ise.expr))).toMap
            val colSet = cols.map(col => col.name).toSet
            val newAnno = invisScm.map(_._2).flatMap(ise => {
              if(colSet.contains(ise.name)){
                None
              }
              else
                subj match {
                  case Table(_,_,_,_) => Some(ProjectArg(ise.name, ise.expr)  )
                  case _ => Some(ProjectArg(ise.name, ise.expr))
                }
            })
            
            val annotatedOp = Project(cols.map(col => {
              val projArg = {
                if(repAnno.contains(col.name)){
                  repAnno(col.name)
                }
                else{
                  col
                }
              }
              ProjectArg(projArg.name, projArg.expression)  
            }).union(newAnno), generalAnnotationsAndRecoveryToProjections(subj) )
            annotatedOp
      }
      /*case Aggregate(groupBy, agggregates, Project(cols, src@Annotate(subj,invisScm))) => {
            val repAnno = invisScm.map(_._2).map(ise => (ise.name, ProjectArg(ise.name, ise.expr))).toMap
            val colSet = cols.map(col => col.name).toSet
            val (newAnnoProj, newAnnoAgg) = invisScm.map(_._2).flatMap(ise => {
              if(colSet.contains(ise.name)){
                None
              }
              else
                Some((ProjectArg(ise.name, ise.expr), AggFunction("FIRST", false, Seq(ise.expr), ise.name))) 
            }).unzip
            
            val annotatedProjOp = Project(cols.map(col => {
              val projArg = {
                if(repAnno.contains(col.name)){
                  repAnno(col.name)
                }
                else{
                  col
                }
              }
              ProjectArg(projArg.name, projArg.expression)  
            }).union(newAnnoProj), generalAnnotationsAndRecoveryToProjections(subj) )
            Aggregate(groupBy, agggregates ++ newAnnoAgg, annotatedProjOp)
      }*/
      case Project(cols, src) => {
        /*val newCols = cols.map(projArg => {
          ProjectArg(projArg.name, replaceRowIdVars(projArg.expression, "MIMIR_ROWID"))
        })*/
        Project(cols, generalAnnotationsAndRecoveryToProjections(src))
      }
      /*case Aggregate(groupBy, agggregates, source) => {
        //val provSrc  = compileProvenanceWithGProM(source)._1
        //Aggregate(groupBy, agggregates, annotationsAndRecoveryToProjections(provSrc)) 
        Aggregate(groupBy, agggregates, generalAnnotationsAndRecoveryToProjections(source))
      }*/
      case Select(cond, source) => {
         Select(cond,generalAnnotationsAndRecoveryToProjections(source))
      }
      case Join(lhs, rhs) => {
        Join(generalAnnotationsAndRecoveryToProjections(lhs), generalAnnotationsAndRecoveryToProjections(rhs))
      }
      case LeftOuterJoin(lhs, rhs, cond) => {
        LeftOuterJoin(generalAnnotationsAndRecoveryToProjections(lhs), generalAnnotationsAndRecoveryToProjections(rhs), cond)
      }
      case Limit(off,lim,query) => {
        Limit(off,lim,generalAnnotationsAndRecoveryToProjections(query))
      }
      case Union(lhs, rhs) => {
        Union(generalAnnotationsAndRecoveryToProjections(lhs), generalAnnotationsAndRecoveryToProjections(rhs))
      }
      case Sort(sortCols, source) => {
         Sort(sortCols,generalAnnotationsAndRecoveryToProjections(source))
      }
      case x => {
        //println("Op ---------------------------------")
        //println(x)
        x
      }
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
