package mimir.algebra.spark

import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.Dataset
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.{ArrayType, Metadata, DataType, DoubleType, LongType, FloatType, BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.catalyst.expressions.{
  RowNumber,
  MonotonicallyIncreasingID,
  NamedExpression,
  AttributeReference,
  Alias,
  SortOrder,
  Ascending,
  Descending,
  GreaterThanOrEqual,
  Literal,
  Add,
  Subtract,
  Multiply,
  Divide,
  BitwiseAnd,
  BitwiseOr,
  And,
  Or,
  ShiftLeft,
  ShiftRight,
  LessThan,
  LessThanOrEqual,
  GreaterThan,
  EqualTo,
  IsNull,
  Like,
  If,
  ScalaUDF,
  Rand,
  Randn,
  SparkPartitionID,
  WindowSpec,
  WindowSpecDefinition,
  WindowExpression,
  UnspecifiedFrame
}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.{TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogStorageFormat}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedRelation, UnresolvedInlineTable, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.plans.{JoinType, Inner,LeftOuter, Cross}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression,AggregateFunction,AggregateMode,Complete,Count,Average,Sum,First,Max,Min}

import org.apache.spark.sql.execution.datasources.{DataSource, FailureSafeParser}
import scala.language.existentials

import mimir.algebra._
import mimir.Database
import org.apache.spark.sql.types._
import mimir.provenance.Provenance
import org.apache.spark.sql.expressions.Window
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import mimir.backend.sqlite.VGTermFunctions
import mimir.backend.sqlite.BestGuessVGTerm
import mimir.ctables.vgterm.BestGuess
import mimir.ctables.vgterm.IsAcknowledged
import org.apache.spark.sql.catalyst.expressions.Concat
import mimir.models.Model
import org.apache.spark.sql.catalyst.expressions.Substring
import org.apache.spark.sql.catalyst.expressions.SubstringIndex
import org.apache.spark.sql.catalyst.expressions.StringTrim
import org.apache.spark.sql.catalyst.expressions.StringTrimLeft
import org.apache.spark.sql.catalyst.expressions.StartsWith
import org.apache.spark.sql.catalyst.expressions.Contains
import mimir.algebra.function.FunctionRegistry
import mimir.algebra.function.{NativeFunction,ExpressionFunction,FoldFunction}

import mimir.backend.SparkBackend
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.CreateStruct
import org.apache.spark.sql.catalyst.expressions.GetStructField
import org.apache.spark.sql.catalyst.plans.UsingJoin
import mimir.algebra.function.RegisteredFunction
import org.apache.spark.sql.catalyst.expressions.ConcatWs
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectList
import mimir.ctables.vgterm.Sampler
import java.sql.Date
import java.sql.Timestamp
import mimir.util.SparkUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.expressions.aggregate.StddevSamp
import mimir.backend.BackendWithSparkContext
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.CreateArray
import org.apache.spark.sql.types.TypeCollection

object OperatorTranslation
  extends LazyLogging
{
  var db: mimir.Database = null
  def apply(db: mimir.Database) = {
    this.db = db
  }
  
  def mimirOpToSparkOp(oper:Operator) : LogicalPlan = {
    oper match {
      case Project(cols, src) => {
  			org.apache.spark.sql.catalyst.plans.logical.Project(cols.map(col => {
          mimirExprToSparkNamedExpr(src, col.name, col.expression)
        }), mimirOpToSparkOp(src))
			}
			case Aggregate(groupBy, aggregates, source) => {
			  org.apache.spark.sql.catalyst.plans.logical.Aggregate(
          groupBy.map(mimirExprToSparkExpr(source,_)),
          groupBy.map( gb => Alias( UnresolvedAttribute.quoted(gb.id), gb.id)()) ++ aggregates.map( mimirAggFunctionToSparkNamedExpr(source,_)),
          mimirOpToSparkOp(source))
			}
			/*case Select(condition, join@Join(lhs, rhs)) => {
			  org.apache.spark.sql.catalyst.plans.logical.Join( mimirOpToSparkOp(lhs),mimirOpToSparkOp(rhs), Inner, Some(mimirExprToSparkExpr(join,condition)))
			  //makeSparkJoin(lhs, rhs, Some(mimirExprToSparkExpr(oper,condition)), Inner) 
			}*/
			case Select(cond, src) => {
			  org.apache.spark.sql.catalyst.plans.logical.Filter(
			      mimirExprToSparkExpr(oper,cond), mimirOpToSparkOp(src))
			}
			case LeftOuterJoin(lhs, rhs, cond@BoolPrimitive(_)) => {
			  org.apache.spark.sql.catalyst.plans.logical.Join( mimirOpToSparkOp(lhs),mimirOpToSparkOp(rhs), Cross, Some(mimirExprToSparkExpr(oper,cond)))
			}
			case LeftOuterJoin(lhs, rhs, condition) => {
			  //org.apache.spark.sql.catalyst.plans.logical.Join( mimirOpToSparkOp(lhs),mimirOpToSparkOp(rhs), LeftOuter, Some(mimirExprToSparkExpr(oper,condition)))
			  val joinType = LeftOuter
			  makeSparkJoin(lhs, rhs, Some(mimirExprToSparkExpr(oper,condition)), joinType) 
			}
			case Join(lhs, rhs) => {
			  org.apache.spark.sql.catalyst.plans.logical.Join( mimirOpToSparkOp(lhs),mimirOpToSparkOp(rhs), Cross, None)
			  //makeSparkJoin(lhs, rhs, None, Cross) 
			}
			case Union(lhs, rhs) => {
			  org.apache.spark.sql.catalyst.plans.logical.Union(mimirOpToSparkOp(lhs),mimirOpToSparkOp(rhs))
			}
			case Limit(offset, limit, query) => {
			  //TODO: Need a better way to do offsets in spark. This way works without breaking, but it is almost as 
			  //  expensive as the full query for small datasets and more expensive for large datasets
			  val sparkChildOp = mimirOpToSparkOp(query)
			  val offsetOp = if(offset > 0){
			    //the records to exclude
			    val offsetExcept = org.apache.spark.sql.catalyst.plans.logical.Limit(
  			      Literal(offset.toInt), 
  			      sparkChildOp)
  			  //TODO: Except will mess up the order of the Rows in the resulting Dataframe
  			  //org.apache.spark.sql.catalyst.plans.logical.Sort(
			      //Seq(SortOrder(UnresolvedAttribute("MIMIR_ROWID"), Ascending)), true,
			      org.apache.spark.sql.catalyst.plans.logical.Except(sparkChildOp, offsetExcept, true)
			    //)
			  } else {
          sparkChildOp
        }
			  limit match {
			    case Some(limitI) if limitI > 0 => org.apache.spark.sql.catalyst.plans.logical.Limit(
  			      Literal(limitI.toInt), 
  			      offsetOp)
			    case _ => offsetOp
			  }
			}
			case Table(name, alias, sch, meta) => {
			  /*val table = CatalogTable(
          TableIdentifier(name),
          CatalogTableType.EXTERNAL,
          CatalogStorageFormat.empty,
          StructType(sch.map(col => StructField(col._1, getSparkType(col._2), true))) )*/
			  val tableId = TableIdentifier(name.id)
        val plan = UnresolvedRelation(tableId)
        val baseRelation  = if (!alias.equals(name)) {
          SubqueryAlias(alias.id, plan)
        } else {
          plan
        }
			  
			  //here we check if the real table schema matches the table op schema 
			  // because the table op schema may have been rewritten by deepRenameColumn
			  // - like when there is a join with conflicts
			  val realSchema = db.backend.getTableSchema(name).getOrElse(throw new Exception(s"Cannot get schema for table: $name" ))
			  val requireProjection = 
			  sch.zip(realSchema).flatMap {
			    case (tableSchEl, realSchEl) => {
			      if(tableSchEl._1.equals(realSchEl._1))
			        None
			      else
			        Some((tableSchEl._1,realSchEl._1))
			    }
			  }.toMap
			  
			  //TODO: just ignore the prov meta here instead of all of it
			  /*val meta = db.views.get(name) match {
			    case Some(vmeta) if vmeta.isMaterialized => {
			     Seq(("MIMIR_ROWID",Var("MIMIR_ROWID_0"),TRowId()))
			    }
			    case _ => metain  
			  }*/
			  //if there were no table op schema rewrites then just return the table
			  // otherwise add a projection that renames  
			  if(requireProjection.isEmpty) 
			    meta match {
  			    case Seq() => baseRelation
  			    case _ => {
  			      org.apache.spark.sql.catalyst.plans.logical.Project(sch.map(col => {
                mimirExprToSparkNamedExpr(oper, col._1, Var(col._1))
              }) ++ meta.filterNot(el => sch.unzip._1.contains(el._1)).distinct.map(metaEl => mimirExprToSparkNamedExpr(oper, metaEl._1, metaEl._2)) ,baseRelation)
  			    }
  			  }
			  else
  			  org.apache.spark.sql.catalyst.plans.logical.Project(
            oper.columnNames.filterNot(el => meta.unzip3._1.contains(el)).map { col =>
              requireProjection.get(col) match {  
                case Some(target) => mimirExprToSparkNamedExpr(oper, col, Var(target)) 
                case None => mimirExprToSparkNamedExpr(oper, col, Var(col)) 
                }
            } ++ meta.filterNot(el => sch.unzip._1.contains(el._1)).filterNot(el => realSchema.unzip._1.contains(el._1)).distinct.map(metaEl => mimirExprToSparkNamedExpr(oper, metaEl._1, metaEl._2)),
            baseRelation
          )
			  //LogicalRelation(baseRelation, table)
			}
			case View(name, query, annotations) => {
			  val schema = db.typechecker.schemaOf(query)
			  val table = CatalogTable(
          TableIdentifier(name.id),
          CatalogTableType.VIEW,
          CatalogStorageFormat.empty,
          mimirSchemaToStructType(schema) )
        org.apache.spark.sql.catalyst.plans.logical.View( table,
          schema.map(col => {
            AttributeReference(col._1.id, getSparkType(col._2), true, Metadata.empty)( )
          }),
          mimirOpToSparkOp(query))
			}
      case av@AdaptiveView(schemaName, name, query, annotations) => {
        val schema = db.typechecker.schemaOf(av)
        val table = CatalogTable(
          TableIdentifier(name.id),
          CatalogTableType.VIEW,
          CatalogStorageFormat.empty,
          mimirSchemaToStructType(schema))
			  org.apache.spark.sql.catalyst.plans.logical.View( table,
          schema.map(col => {
            AttributeReference(col._1.id, getSparkType(col._2), true, Metadata.empty)( )
          }),
          mimirOpToSparkOp(query))
      }
      case HardTable(schema, data)=> {
        //UnresolvedInlineTable( schema.unzip._1, data.map(row => row.map(mimirExprToSparkExpr(oper,_))))
        /*LocalRelation(mimirSchemaToStructType(schema).map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()), 
            data.map(row => InternalRow(row.map(mimirPrimitiveToSparkInternalRowValue(_)):_*)))*/
        LocalRelation.fromExternalRows(mimirSchemaToStructType(schema).map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()), 
            data.map(row => Row(row.map(mimirPrimitiveToSparkExternalRowValue(_)):_*)))     
      }
			case Sort(sortCols, src) => {
			  org.apache.spark.sql.catalyst.plans.logical.Sort(
          sortCols.map(sortCol => SortOrder(mimirExprToSparkExpr(oper,sortCol.expression),if(sortCol.ascending) Ascending else Descending)),
          true,
          mimirOpToSparkOp(src))
			}
			case _ => {
			  throw new Exception("Operator Translation not implemented '"+oper+"'")
			}
    }
  }
  
  def queryExecution(plan:LogicalPlan ) = {
   db.backend match {
     case sparkBackend:SparkBackend => sparkBackend.sparkSql.sparkSession.sessionState.executePlan(plan) 
     case x => throw new Exception("Cant get Query Execution From backend of type: " + x.getClass.getName) 
   }
  }
  
  def dataset(plan:LogicalPlan) = {
    val qe = queryExecution(plan)
    qe.assertAnalyzed()
    val sparkSession = db.backend match {
     case sparkBackend:SparkBackend => sparkBackend.sparkSql.sparkSession 
     case x => throw new Exception("Cant get Dataset From backend of type: " + x.getClass.getName) 
   }
    new Dataset[Row](sparkSession, plan, RowEncoder(qe.analyzed.schema)) 
  }
  
  def makeInitSparkJoin(left: LogicalPlan, right: LogicalPlan, usingColumns: Seq[String], joinType: JoinType): LogicalPlan = {
    // Analyze the self join. The assumption is that the analyzer will disambiguate left vs right
    // by creating a new instance for one of the branch.
    val joined = db.backend.asInstanceOf[BackendWithSparkContext].getSparkContext().sparkSession.sessionState.executePlan(
       org.apache.spark.sql.catalyst.plans.logical.Join(left, right, joinType = joinType, None))
      .analyzed.asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Join]

    org.apache.spark.sql.catalyst.plans.logical.Join(
        joined.left,
        joined.right,
        UsingJoin(joinType, usingColumns),
        None)
    
  }
  
  def makeSparkJoin(lhs:Operator, rhs:Operator, condition:Option[org.apache.spark.sql.catalyst.expressions.Expression], joinType:JoinType): LogicalPlan = {
    val (lplan, rplan) = ( mimirOpToSparkOp(lhs),mimirOpToSparkOp(rhs))
	  val (lqe, rqe) = (dataset(lplan), dataset(rplan))
	  
	  val columnAmbiguities = lqe.schema.intersect(rqe.schema)
	  
	  val joined:org.apache.spark.sql.catalyst.plans.logical.Join = if(!columnAmbiguities.isEmpty){
	    val tjoin = makeInitSparkJoin(lplan, rplan, columnAmbiguities.map(_.name), joinType).asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Join]
      val cond = condition.map { _.transform {
        case org.apache.spark.sql.catalyst.expressions.EqualTo(a: AttributeReference, b: AttributeReference)
            if a.sameRef(b) =>
          org.apache.spark.sql.catalyst.expressions.EqualTo(
            tjoin.left.resolve(Seq(a.name), db.backend.asInstanceOf[BackendWithSparkContext].getSparkContext().sparkSession.sessionState.analyzer.resolver).get,//p(withPlan(logicalPlan.left))('resolve)(a.name).asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression],
            tjoin.right.resolve(Seq(b.name), db.backend.asInstanceOf[BackendWithSparkContext].getSparkContext().sparkSession.sessionState.analyzer.resolver).get)//p(withPlan(logicalPlan.right))('resolve)(b.name).asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression])
      }}
      tjoin.copy(condition = cond)
	  }
	  else   
  	  // Creates a Join node and resolve it first, to get join condition resolved, self-join resolved,
      // etc.
      //val joined = db.backend.asInstanceOf[BackendWithSparkContext].getSparkContext().sparkSession.sessionState.executePlan(
        org.apache.spark.sql.catalyst.plans.logical.Join(
          lplan,
          rplan,
          joinType,
          condition)//).analyzed.asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Join]
  
    // For both join side, combine all outputs into a single column and alias it with "_1" or "_2",
    // to match the schema for the encoder of the join result.
    // Note that we do this before joining them, to enable the join operator to return null for one
    // side, in cases like outer-join.
    val left = {
      val combined = if (getPrivateMamber[ExpressionEncoder[Row]](dataset(lplan),"exprEnc").flat) {
        assert(joined.left.output.length == 1)
        Alias(joined.left.output.head, "_1")()
      } else {
        Alias(CreateStruct(joined.left.output), "_1")()
      }
      org.apache.spark.sql.catalyst.plans.logical.Project(combined :: Nil, joined.left)
    }

    val right = {
      val combined = if (getPrivateMamber[ExpressionEncoder[Row]](dataset(rplan),"exprEnc").flat) {
        assert(joined.right.output.length == 1)
        Alias(joined.right.output.head, "_2")()
      } else {
        Alias(CreateStruct(joined.right.output), "_2")()
      }
      org.apache.spark.sql.catalyst.plans.logical.Project(combined :: Nil, joined.right)
    }

    // Rewrites the join condition to make the attribute point to correct column/field, after we
    // combine the outputs of each join side.
   /* val conditionExpr = joined.condition match { 
      case Some(c) => Some(c transformUp {
        case a: org.apache.spark.sql.catalyst.expressions.Attribute if joined.left.outputSet.contains(a) =>
          if (getPrivateMamber[ExpressionEncoder[Row]](dataset(lplan),"exprEnc").flat) {
            left.output.head
          } else {
            val index = joined.left.output.indexWhere(_.exprId == a.exprId)
            GetStructField(left.output.head, index)
          }
        case a: org.apache.spark.sql.catalyst.expressions.Attribute if joined.right.outputSet.contains(a) =>
          if (getPrivateMamber[ExpressionEncoder[Row]](dataset(rplan),"exprEnc").flat) {
            right.output.head
          } else {
            val index = joined.right.output.indexWhere(b => {
              if(!b.resolved){
                logger.debug(s"--------> output attr not resolved: ${b.name}")
                logger.debug(joined.right)
                joined.right.resolve(Seq(b.name), db.backend.asInstanceOf[BackendWithSparkContext].getSparkContext().sparkSession.sessionState.analyzer.resolver).get
              }
              b.exprId == a.exprId
            })
            GetStructField(right.output.head, index)
          }
      })
      case None => None
    }

    org.apache.spark.sql.catalyst.plans.logical.Join(left, right, joined.joinType, conditionExpr)*/ joined
  }
  
  /*def makeSparkJoin(lhs:Operator, rhs:Operator, condition:Option[org.apache.spark.sql.catalyst.expressions.Expression], joinType:JoinType) : LogicalPlan = {
    val (lplan, rplan) = ( mimirOpToSparkOp(lhs),mimirOpToSparkOp(rhs))
	  val logicalPlan:org.apache.spark.sql.catalyst.plans.logical.Join = org.apache.spark.sql.catalyst.plans.logical.Join(lplan, rplan, joinType, condition)
	  // If left/right have no output set intersection, return the plan.
    
	  val lanalyzed = queryExecution(lplan).analyzed //withPlan(lplan).queryExecution.analyzed
    val ranalyzed = queryExecution(rplan).analyzed//withPlan(rplan).queryExecution.analyzed
    if (lanalyzed.outputSet.intersect(ranalyzed.outputSet).isEmpty) {
      return logicalPlan
    }

    // Otherwise, find the trivially true predicates and automatically resolves them to both sides.
    // By the time we get here, since we have already run analysis, all attributes should've been
    // resolved and become AttributeReference.
	  val cond = logicalPlan.condition.map { _.transform {
      case org.apache.spark.sql.catalyst.expressions.EqualTo(a: AttributeReference, b: AttributeReference)
          if a.sameRef(b) =>
        org.apache.spark.sql.catalyst.expressions.EqualTo(
          logicalPlan.left.resolve(Seq(a.name), db.backend.asInstanceOf[BackendWithSparkContext].getSparkContext().sparkSession.sessionState.analyzer.resolver).get,//p(withPlan(logicalPlan.left))('resolve)(a.name).asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression],
          logicalPlan.right.resolve(Seq(b.name), db.backend.asInstanceOf[BackendWithSparkContext].getSparkContext().sparkSession.sessionState.analyzer.resolver).get)//p(withPlan(logicalPlan.right))('resolve)(b.name).asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression])
    }}

    logicalPlan.copy(condition = cond)
  }*/
  
  def getPrivateMamber[T](inst:AnyRef, fieldName:String) : T = {
    def _parents:scala.collection.immutable.Stream[Class[_]] = Stream(inst.getClass) #::: _parents.map(_.asInstanceOf[Class[_]].getSuperclass)
    val parents = _parents.takeWhile(_ != null).toList
    val fields = parents.flatMap(_.getDeclaredFields())
    val field = fields.find(_.getName == fieldName).getOrElse(throw new IllegalArgumentException("Field " + fieldName + " not found"))
    field.setAccessible(true)
    field.get(inst).asInstanceOf[T]
  }
  
  //some stuff for calling spark internals
  class PrivateMethodCaller(x: AnyRef, methodName: String) {
    def apply(_args: Any*): Any = {
      val args = _args.map(_.asInstanceOf[AnyRef])
      def _parents: Stream[Class[_]] = Stream(x.getClass) #::: _parents.map(_.getSuperclass)
      val parents = _parents.takeWhile(_ != null).toList
      val methods = parents.flatMap(_.getDeclaredMethods)
      val method = methods.find(_.getName == methodName).getOrElse(throw new IllegalArgumentException("Method " + methodName + " not found"))
      method.setAccessible(true)
      logger.debug("=======================================")
      logger.debug(s"$method")
      logger.debug(s"$x")
      logger.debug(s"$args")
      logger.debug("=======================================")
      method.invoke(x, args : _*)
    }
  }
  
  class PrivateMethodExposer(x: AnyRef) {
    def apply(method: scala.Symbol): PrivateMethodCaller = new PrivateMethodCaller(x, method.name)
  }

  def p(x: AnyRef): PrivateMethodExposer = new PrivateMethodExposer(x)
  
  def companionObj[T](name:String)(implicit man: scala.reflect.Manifest[T]) = { 
    val c = Class.forName(name + "$")
    c.getField("MODULE$").get(c)
  }
  
  private def withPlan(logicalPlan: LogicalPlan): DataFrame = {
    try{
      val runtimeMirror = scala.reflect.runtime.universe.runtimeMirror(getClass.getClassLoader)
      val module = runtimeMirror.staticModule("org.apache.spark.sql.Dataset")
      val obj = runtimeMirror.reflectModule(module)
      val someTrait = obj.instance.asInstanceOf[AnyRef]
      
      //val someTrait = companionObj[org.apache.spark.sql.Dataset[Row]]("org.apache.spark.sql.Dataset")
      logger.debug(s"$someTrait")
      p(someTrait)('ofRows)(db.backend.asInstanceOf[BackendWithSparkContext].getSparkContext().sparkSession, logicalPlan).asInstanceOf[DataFrame]
    
      //new Dataset[Row](db.backend.asInstanceOf[BackendWithSparkContext].getSparkContext().sparkSession, logicalPlan, RowEncoder(logicalPlan.schema)).toDF()
    }
    catch {
      case t: Throwable => {
        logger.debug("-------------------------> Exception Executing Spark Op withPlan: ")
        logger.debug("------------------------ spark op --------------------------")
        logger.debug(s"$logicalPlan")
        logger.debug("------------------------------------------------------------")
        throw t
      }
    }
  }
 
  
  def mimirExprToSparkNamedExpr(oper:Operator, name:ID, expr:Expression) : NamedExpression = {
    Alias(mimirExprToSparkExpr(oper,expr),name.id)()
  }
  
  def mimirAggFunctionToSparkNamedExpr(oper:Operator, aggr:AggFunction) : NamedExpression = {
    Alias(AggregateExpression(
      aggr.function.id match {
        case "count" => 
          aggr.args match {
            case Seq() => Count(Seq(Literal(1)))
            case _ => Count(aggr.args.map(mimirExprToSparkExpr(oper,_)))
          }
        case "avg" => {
          Average(mimirExprToSparkExpr(oper,aggr.args.head))
        }
        case "sum" => {
          Sum(mimirExprToSparkExpr(oper,aggr.args.head))
        }
        case "stddev" => {
          StddevSamp(mimirExprToSparkExpr(oper,aggr.args.head))
        }
        case "first" => {
          First(mimirExprToSparkExpr(oper,aggr.args.head),mimirExprToSparkExpr(oper,BoolPrimitive(true)))
        }
        case "max" => {
          Max(mimirExprToSparkExpr(oper,aggr.args.head))
        }
        case "min" => {
          Min(mimirExprToSparkExpr(oper,aggr.args.head))
        }
        case "group_and" => {
          GroupAnd(mimirExprToSparkExpr(oper,aggr.args.head))
        }
        case "group_or" => {
          GroupOr(mimirExprToSparkExpr(oper,aggr.args.head))
        }
        case "group_bitwise_or" => {
          GroupBitwiseOr(mimirExprToSparkExpr(oper,aggr.args.head))
        }
        case "group_bitwise_and" => {
          GroupBitwiseAnd(mimirExprToSparkExpr(oper,aggr.args.head))
        }
        case "json_group_array" => {
          JsonGroupArray(mimirExprToSparkExpr(oper,aggr.args.head))
          //TODO: when we add TArray() type we can use this
          //CollectList(mimirExprToSparkExpr(oper,aggr.args.head))
        }
        case function => {
          val fi = db.backend.asInstanceOf[SparkBackend].sparkSql.sparkSession.sessionState.catalog.lookupFunctionInfo(FunctionIdentifier(function.toLowerCase()))
          val sparkInputs = aggr.args.map(inp => mimirExprToSparkExpr(oper, inp))
          val constructorTypes = aggr.args.map(inp => classOf[org.apache.spark.sql.catalyst.expressions.Expression])
          Class.forName(fi.getClassName).getDeclaredConstructor(constructorTypes:_*).newInstance(sparkInputs:_*)
                           .asInstanceOf[org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction]
          //throw new Exception("Aggregate Function Translation not implemented '"+function+"'")
        } 
      },
      Complete,
      aggr.distinct
    ), aggr.alias.id)()
  }
  
  
  def mimirExprToSparkExpr(oper:Operator, expr:Expression) : org.apache.spark.sql.catalyst.expressions.Expression = {
    expr match {
      case primitive : PrimitiveValue => {
        mimirPrimitiveToSparkPrimitive(primitive)
      }
      case cmp@Comparison(op,lhs,rhs) => {
        mimirComparisonToSparkComparison(oper, cmp)
      }
      case arith@Arithmetic(op,lhs,rhs) => {
        mimirArithmeticToSparkArithmetic(oper, arith)
      }
      case cnd@Conditional(condition,thenClause,elseClause) => {
        mimirConditionalToSparkConditional(oper, cnd)
      }
      case Var(name) if name.equals("ROWID") => {
        org.apache.spark.sql.catalyst.expressions.Cast(Alias(Add(MonotonicallyIncreasingID(), Literal(1)),"ROWID")(),getSparkType(db.backend.rowIdType),None)
      }
      case Var(v) => {
        UnresolvedAttribute.quoted(v.id)
      }
      case rid@RowIdVar() => {
        //UnresolvedAttribute("ROWID")
        org.apache.spark.sql.catalyst.expressions.Cast(Alias(Add(MonotonicallyIncreasingID(), Literal(1)),RowIdVar().toString())(),getSparkType(db.backend.rowIdType),None)
      }
      case func@Function(_,_) => {
        mimirFunctionToSparkFunction(oper, func)
      }
      case CastExpression(expr, t) => {
        org.apache.spark.sql.catalyst.expressions.Cast(mimirExprToSparkExpr(oper,expr), getSparkType(t), None)
      }
      case BestGuess(model, idx, args, hints) => {
        val name = model.name
        //logger.debug(s"-------------------Translate BestGuess VGTerm($name, $idx, (${args.mkString(",")}), (${hints.mkString(",")}))")
       BestGuessUDF(oper, model, idx, args, hints).getUDF
        //UnresolvedFunction(mimir.ctables.CTables.FN_BEST_GUESS, mimirExprToSparkExpr(oper,StringPrimitive(name)) +: mimirExprToSparkExpr(oper,IntPrimitive(idx)) +: (args.map(mimirExprToSparkExpr(oper,_)) ++ hints.map(mimirExprToSparkExpr(oper,_))), true )
      }
      case IsAcknowledged(model, idx, args) => {
        val name = model.name
        //logger.debug(s"-------------------Translate IsAcknoledged VGTerm($name, $idx, (${args.mkString(",")}))")
        AckedUDF(oper, model, idx, args).getUDF
        //UnresolvedFunction(mimir.ctables.CTables.FN_IS_ACKED, mimirExprToSparkExpr(oper,StringPrimitive(name)) +: mimirExprToSparkExpr(oper,IntPrimitive(idx)) +: (args.map(mimirExprToSparkExpr(oper,_)) ), true )
      }
      case Sampler(model, idx, args, hints, seed) => {
        SampleUDF(oper, model, idx, seed, args, hints).getUDF
      }
      case VGTerm(name, idx, args, hints) => { //default to best guess
        //logger.debug(s"-------------------Translate VGTerm($name, $idx, (${args.mkString(",")}), (${hints.mkString(",")}))")
        val model = db.models.get(name)
        BestGuessUDF(oper, model, idx, args, hints).getUDF
        //UnresolvedFunction(mimir.ctables.CTables.FN_BEST_GUESS, mimirExprToSparkExpr(oper,StringPrimitive(name)) +: mimirExprToSparkExpr(oper,IntPrimitive(idx)) +: (args.map(mimirExprToSparkExpr(oper,_)) ++ hints.map(mimirExprToSparkExpr(oper,_))), true )
      }
      case DataWarning(_, v, _, _) => {
        mimirExprToSparkExpr(oper, v)
      }
      case IsNullExpression(iexpr) => {
        IsNull(mimirExprToSparkExpr(oper,iexpr))
      }
      case Not(nexpr) => {
        org.apache.spark.sql.catalyst.expressions.Not(mimirExprToSparkExpr(oper,nexpr))
      }
      case (JDBCVar(_) | _:Proc) => {
        throw new RAException("Spark doesn't support: "+expr)
      }
    }
  }
  
  val defaultDate = DateTimeUtils.toJavaDate(0)
  val defaultTimestamp = DateTimeUtils.toJavaTimestamp(0L)
  def mimirPrimitiveToSparkPrimitive(primitive : PrimitiveValue) : Literal = {
    primitive match {
      case NullPrimitive() => Literal(null)
      case RowIdPrimitive(s) => Literal(s)
      case StringPrimitive(s) => Literal(s)
      case IntPrimitive(i) => Literal(i)
      case FloatPrimitive(f) => Literal(f)
      case BoolPrimitive(b) => Literal(b)
      case ts@TimestampPrimitive(y,m,d,h,mm,s,ms) => Literal.create(SparkUtils.convertTimestamp(ts), TimestampType)
      case dt@DatePrimitive(y,m,d) => Literal.create(SparkUtils.convertDate(dt), DateType)
      case x =>  Literal(x.asString)
    }
  }
  
  def mimirPrimitiveToSparkInternalRowValue(primitive : PrimitiveValue) : Any = {
    primitive match {
      case NullPrimitive() => null
      case RowIdPrimitive(s) => UTF8String.fromString(s)
      case StringPrimitive(s) => UTF8String.fromString(s)
      case IntPrimitive(i) => i
      case FloatPrimitive(f) => f
      case BoolPrimitive(b) => b
      case ts@TimestampPrimitive(y,m,d,h,mm,s,ms) => SparkUtils.convertTimestamp(ts)//DateTimeUtils.fromJavaTimestamp(SparkUtils.convertTimestamp(ts))
      case dt@DatePrimitive(y,m,d) => SparkUtils.convertDate(dt)//DateTimeUtils.fromJavaDate(SparkUtils.convertDate(dt))
      case x =>  UTF8String.fromString(x.asString)
    }
  }
  
  def mimirPrimitiveToSparkExternalRowValue(primitive : PrimitiveValue) : Any = {
    primitive match {
      case null => null
      case NullPrimitive() => null
      case RowIdPrimitive(s) => s
      case StringPrimitive(s) => s
      case IntPrimitive(i) => i
      case FloatPrimitive(f) => f
      case BoolPrimitive(b) => b
      case ts@TimestampPrimitive(y,m,d,h,mm,s,ms) => SparkUtils.convertTimestamp(ts)
      case dt@DatePrimitive(y,m,d) => SparkUtils.convertDate(dt)
      case x =>  x.asString
    }
  }
  
  def mimirPrimitiveToSparkInternalInlineFuncParam(primitive : PrimitiveValue) : Any = {
    primitive match {
      case IntPrimitive(i) => i.toInt
      case x =>  mimirPrimitiveToSparkInternalRowValue(x)
    }
  }
  
  def mimirPrimitiveToSparkExternalInlineFuncParam(primitive : PrimitiveValue) : Any = {
    primitive match {
      case IntPrimitive(i) => i.toInt
      case x =>  mimirPrimitiveToSparkExternalRowValue(x)
    }
  }
  
  def mimirComparisonToSparkComparison(oper:Operator, cmp:Comparison) : org.apache.spark.sql.catalyst.expressions.Expression = {
    cmp.op match {
      case  Cmp.Eq => EqualTo(mimirExprToSparkExpr(oper,cmp.lhs), mimirExprToSparkExpr(oper,cmp.rhs))
      case  Cmp.Neq  => org.apache.spark.sql.catalyst.expressions.Not(EqualTo(mimirExprToSparkExpr(oper,cmp.lhs), mimirExprToSparkExpr(oper,cmp.rhs))) 
      case  Cmp.Gt  => GreaterThan(mimirExprToSparkExpr(oper,cmp.lhs), mimirExprToSparkExpr(oper,cmp.rhs))
      case  Cmp.Gte  => GreaterThanOrEqual(mimirExprToSparkExpr(oper,cmp.lhs), mimirExprToSparkExpr(oper,cmp.rhs))
      case  Cmp.Lt  => LessThan(mimirExprToSparkExpr(oper,cmp.lhs), mimirExprToSparkExpr(oper,cmp.rhs))
      case  Cmp.Lte  => LessThanOrEqual(mimirExprToSparkExpr(oper,cmp.lhs), mimirExprToSparkExpr(oper,cmp.rhs))
      case  Cmp.Like  => Like(mimirExprToSparkExpr(oper,cmp.lhs), mimirExprToSparkExpr(oper,cmp.rhs)) 
      case  Cmp.NotLike => org.apache.spark.sql.catalyst.expressions.Not(Like(mimirExprToSparkExpr(oper,cmp.lhs), mimirExprToSparkExpr(oper,cmp.rhs)))
      case x => throw new Exception("Invalid operand '"+x+"'")
    }
  }
  
  def mimirArithmeticToSparkArithmetic(oper:Operator, arith:Arithmetic) : org.apache.spark.sql.catalyst.expressions.Expression = {
    arith.op match {
      case  Arith.Add => Add(mimirExprToSparkExpr(oper,arith.lhs),mimirExprToSparkExpr(oper,arith.rhs)) 
      case  Arith.Sub => Subtract(mimirExprToSparkExpr(oper,arith.lhs),mimirExprToSparkExpr(oper,arith.rhs)) 
      case  Arith.Mult => Multiply(mimirExprToSparkExpr(oper,arith.lhs),mimirExprToSparkExpr(oper,arith.rhs)) 
      case  Arith.Div => Divide(mimirExprToSparkExpr(oper,arith.lhs),mimirExprToSparkExpr(oper,arith.rhs))  
      case  Arith.BitAnd => BitwiseAnd(mimirExprToSparkExpr(oper,arith.lhs),mimirExprToSparkExpr(oper,arith.rhs))  
      case  Arith.BitOr => BitwiseOr(mimirExprToSparkExpr(oper,arith.lhs),mimirExprToSparkExpr(oper,arith.rhs))  
      case  Arith.And => And(mimirExprToSparkExpr(oper,arith.lhs),mimirExprToSparkExpr(oper,arith.rhs))   
      case  Arith.Or => Or(mimirExprToSparkExpr(oper,arith.lhs),mimirExprToSparkExpr(oper,arith.rhs))   
      case  Arith.ShiftLeft => ShiftLeft(mimirExprToSparkExpr(oper,arith.lhs),mimirExprToSparkExpr(oper,arith.rhs))
      case  Arith.ShiftRight => ShiftRight(mimirExprToSparkExpr(oper,arith.lhs),mimirExprToSparkExpr(oper,arith.rhs))  
      case x => throw new Exception("Invalid operand '"+x+"'")
    }
  }
  
  def mimirConditionalToSparkConditional(oper:Operator, cnd:Conditional) : org.apache.spark.sql.catalyst.expressions.Expression = {
    cnd match {
      case Conditional(cond, thenClause, elseClause) => {
        If(mimirExprToSparkExpr(oper,cond),mimirExprToSparkExpr(oper,thenClause),mimirExprToSparkExpr(oper,elseClause))
      }
    }
  }
  
  def mimirFunctionToSparkFunction(oper:Operator, func:Function) : org.apache.spark.sql.catalyst.expressions.Expression = {
    val vgtBGFunc = VGTermFunctions.bestGuessVGTermFn
    func.op match {
      case ID("random") => {
        Randn(1L)
      }
      case ID("year") => {
        org.apache.spark.sql.catalyst.expressions.Year(mimirExprToSparkExpr(oper,func.params.head))
      }
      case ID("second") => {
        org.apache.spark.sql.catalyst.expressions.Second(mimirExprToSparkExpr(oper,func.params.head))
      }
      case `vgtBGFunc` => {
        throw new Exception(s"Function Translation not implemented $vgtBGFunc(${func.params.mkString(",")})")
      }
      case ID(name) => {
        FunctionUDF(oper, name, db.functions.get(func.op), func.params, func.params.map(arg => db.typechecker.typeOf(arg, oper))).getUDF
      }
    }
  }
  
  def dataTypeFromString(dataTypeString:String): DataType = {
   dataTypeString match {
      case "BinaryType" => BinaryType
      case "BooleanType" => BooleanType
      case "ByteType" => ByteType
      case "CalendarIntervalType" => CalendarIntervalType
      case "DateType" => DateType
      case "DoubleType" => DoubleType
      case "FloatType" => FloatType
      case "IntegerType" => IntegerType
      case "LongType" => LongType
      case "NullType" => NullType
      case "ShortType" => ShortType
      case "StringType" => StringType
      case "TimestampType" => TimestampType
   }
  }
  
  def dataTypeFromHiveDataTypeString(hiveType:String): DataType = {
    hiveType.toUpperCase() match {
      case "TINYINT" => ShortType
      case "SMALLINT" => IntegerType
      case "INT" => LongType
      case "BIGINT" => LongType 
      case "FLOAT" => FloatType 
      case "DOUBLE" => DoubleType
      case "DECIMAL" => DoubleType
      case "TIMESTAMP" => TimestampType
      case "DATE" => DateType
      case "STRING" => StringType
      case "VARCHAR" => StringType
      case "CHAR" => StringType
      case "BOOLEAN" => BooleanType 
      case "BINARY" => BinaryType
    }
  }
  
  def mimirSchemaToStructType(schema:Seq[(ID, Type)]):StructType = {
    StructType(schema.map(col => StructField(col._1.id, getSparkType(col._2), true)))  
  }
  
  def structTypeToMimirSchema(schema:StructType): Seq[(String, Type)] = {
    schema.fields.map(col => (col.name, getMimirType(col.dataType)))  
  }
  
  def getSparkType(t:Type) : DataType = {
    t match {
      case TInt() => LongType
      case TFloat() => DoubleType
      case TDate() => DateType
      case TString() => StringType
      case TBool() => BooleanType
      case TRowId() => StringType
      case TType() => StringType
      case TAny() => StringType
      case TTimestamp() => TimestampType
      case TInterval() => StringType
      case TUser(name) => getSparkType(mimir.algebra.TypeRegistry.registeredTypes(name)._2)
      case _ => StringType
    }
  }
  
  def getInternalSparkType(t:DataType) : DataType = {
    t match {
      case IntegerType => IntegerType
      case DoubleType => DoubleType
      case FloatType => FloatType
      case LongType => LongType
      case BooleanType => BooleanType
      case DateType => LongType
      case TimestampType => LongType
      case _ => StringType
    }
  }
  
  def getMimirType(dataType: DataType): Type = {
    dataType match {
      case IntegerType => TInt()
      case DoubleType => TFloat()
      case FloatType => TFloat()
      case LongType => TInt()
      case BooleanType => TBool()
      case DateType => TDate()
      case TimestampType => TTimestamp()
      case _ => TString()
    }
  }
  
  def getNative(value:PrimitiveValue, t:Type): Any = {
    value match {
      case NullPrimitive() => t match {
        case TInt() => 0L
        case TFloat() => new java.lang.Double(0.0)
        case TDate() => OperatorTranslation.defaultDate
        case TString() => ""
        case TBool() => new java.lang.Boolean(false)
        case TRowId() => ""
        case TType() => ""
        case TAny() => ""
        case TTimestamp() => OperatorTranslation.defaultTimestamp
        case TInterval() => ""
        case TUser(name) => getNative(value, mimir.algebra.TypeRegistry.registeredTypes(name)._2)
        case x => ""
      }
      case RowIdPrimitive(s) => s
      case StringPrimitive(s) => s
      case IntPrimitive(i) => i
      case FloatPrimitive(f) => new java.lang.Double(f)
      case BoolPrimitive(b) => new java.lang.Boolean(b)
      case ts@TimestampPrimitive(y,m,d,h,mm,s,ms) => SparkUtils.convertTimestamp(ts)
      case dt@DatePrimitive(y,m,d) => SparkUtils.convertDate(dt)
      case x =>  x.asString
    }
  }
  
  def extractTables(oper: Operator): Seq[String] = 
  {
    oper match {
      case Table(name, alias, tgtSch, tgtMetadata) => Seq(name.id)
      case _ => oper.children.map(extractTables(_)).flatten
    }
  }
  
  /*def mimirOpToDF(sqlContext:SQLContext, oper:Operator) : DataFrame = {
    val sparkOper = OperatorTranslation.mimirOpToSparkOp(oper)
    logger.debug("---------------------------- Mimir Oper -----------------------------")
    logger.debug(oper)
    logger.debug("---------------------------- Spark Oper -----------------------------")
    logger.debug(sparkOper)
    logger.debug("---------------------------------------------------------------------")
    
    val sparkTables = sqlContext.sparkSession.catalog.listTables().collect()
    extractTables(oper).map(t => {
      if(!sparkTables.contains(t)){
        logger.debug(s"loading table into spark: $t")
        sqlContext.read.format("jdbc")
          .options( 
            Map(
              "url" -> s"jdbc:sqlite:${db.backend.asInstanceOf[mimir.sql.JDBCBackend].filename}",
              "dbtable" -> t)).load().registerTempTable(t)
      }
      true
    })
    

    val qe = sqlContext.sparkSession.sessionState.executePlan(sparkOper)
    qe.assertAnalyzed()
    new Dataset[Row](sqlContext.sparkSession, OperatorTranslation.mimirOpToSparkOp(oper), RowEncoder(qe.analyzed.schema)).toDF()
    
        
    /*val cls = DataSource.lookupDataSource("jdbc")
    logger.debug(cls.getName)
    sqlContext.sparkSession.baseRelationToDataFrame(
      DataSource.apply(
        sqlContext.sparkSession,
        paths = Seq.empty,
        userSpecifiedSchema = None,
        className = "jdbc",
        options = Map("url" -> "jdbc:sqlite:debug.db") ).resolveRelation())*/
  }*/
  
}

class RowIndexPlan(lp:LogicalPlan,  offset: Long = 1, indexName: String = "index") {
  
  val partOp = org.apache.spark.sql.catalyst.plans.logical.Project(
      Seq(Alias(SparkPartitionID(),"partition_id")(), 
          Alias(MonotonicallyIncreasingID(),"inc_id")()), lp)
  
  val fop =  OperatorTranslation.dataset(org.apache.spark.sql.catalyst.plans.logical.Filter(
      Alias(Add(Subtract(Subtract(WindowExpression(Sum(UnresolvedAttribute("cnt")),WindowSpecDefinition(Seq(), Seq(), UnspecifiedFrame)), 
                  UnresolvedAttribute("cnt")),UnresolvedAttribute("inc_id")),Literal(offset)),"cnt")(), 
      org.apache.spark.sql.catalyst.plans.logical.Sort(Seq(SortOrder(UnresolvedAttribute("partition_id"), Ascending)), true,   
       org.apache.spark.sql.catalyst.plans.logical.Aggregate(
          Seq(UnresolvedAttribute("partition_id")),
          Seq(Alias(Literal(1),"cnt")(), Alias(First(UnresolvedAttribute("inc_id"),Literal(false)),"inc_id")()),
          partOp))
      )).collect().map(_.getLong(0))      
  
  val inFunc = (partitionId:Int) => {
   fop(partitionId)
  }
      
  def getPlan() = {
    org.apache.spark.sql.catalyst.plans.logical.Project(
      lp.schema.fields.map(fld => UnresolvedAttribute(fld.name)) :+ UnresolvedAttribute("indexName"), 
      org.apache.spark.sql.catalyst.plans.logical.Project(
          Seq(Alias(getUDF(),"partition_offset")(), Alias(Add(UnresolvedAttribute("partition_offset"), 
              UnresolvedAttribute("inc_id")),indexName)()), partOp))
  }

  private def getUDF() = {
    ScalaUDF(
      inFunc,
      LongType,
      Seq(UnresolvedAttribute("partition_id")),
      Seq(false),
      Seq(IntegerType),
      Some("mimir_row_index"),true, true)
  }
}

class MimirUDF {
  def getPrimitive(t:Type, value:Any) = value match {
    case null => NullPrimitive()
    case _ => t match {
      //case TInt() => IntPrimitive(value.asInstanceOf[Long])
      case TInt() => IntPrimitive(value.asInstanceOf[Long])
      case TFloat() => FloatPrimitive(value.asInstanceOf[Double])
      case TDate() => SparkUtils.convertDate(value.asInstanceOf[Date])
      case TTimestamp() => SparkUtils.convertTimestamp(value.asInstanceOf[Timestamp])
      case TString() => StringPrimitive(value.asInstanceOf[String])
      case TBool() => BoolPrimitive(value.asInstanceOf[Boolean])
      case TRowId() => RowIdPrimitive(value.asInstanceOf[String])
      case TType() => TypePrimitive(Type.fromString(value.asInstanceOf[String]))
      //case TAny() => NullPrimitive()
      //case TUser(name) => name.toLowerCase
      //case TInterval() => Primitive(value.asInstanceOf[Long])
      case _ => StringPrimitive(value.asInstanceOf[String])
    }
  }
  def getNative(primitive : PrimitiveValue) : AnyRef = 
    primitive match {
      case NullPrimitive() => null
      case RowIdPrimitive(s) => s
      case StringPrimitive(s) => s
      case IntPrimitive(i) => new java.lang.Long(i)
      case FloatPrimitive(f) => new java.lang.Double(f)
      case BoolPrimitive(b) => new java.lang.Boolean(b)
      case ts@TimestampPrimitive(y,m,d,h,mm,s,ms) => SparkUtils.convertTimestamp(ts)
      case dt@DatePrimitive(y,m,d) => SparkUtils.convertDate(dt)
      case x =>  x.asString
    }
  def getStructType(datatypes:Seq[DataType]): StructType = {
    StructType(datatypes.map(dti => StructField("", OperatorTranslation.getInternalSparkType(dti), true)))
  }
}


case class BestGuessUDF(oper:Operator, model:Model, idx:Int, args:Seq[Expression], hints:Seq[Expression]) extends MimirUDF {
  val sparkVarType = OperatorTranslation.getSparkType(model.varType(idx, model.argTypes(idx)))
  val sparkArgs = (args.map(arg => OperatorTranslation.mimirExprToSparkExpr(oper,arg)) ++ hints.map(hint => OperatorTranslation.mimirExprToSparkExpr(oper,hint))).toList.toSeq
  val sparkArgTypes = (model.argTypes(idx).map(arg => OperatorTranslation.getSparkType(arg)) ++ model.hintTypes(idx).map(hint => OperatorTranslation.getSparkType(hint))).toList.toSeq
    
  def extractArgsAndHints(args:Seq[Any]) : (Seq[PrimitiveValue],Seq[PrimitiveValue]) ={
    try{
      val getParamPrimitive:(Type, Any) => PrimitiveValue = if(sparkArgs.length > 22) (t, v) => {
        v match {
          case null => NullPrimitive()
          case _ => Cast(t,StringPrimitive(v.asInstanceOf[String]))
        }
      }
      else getPrimitive _
      val argList =
        model.argTypes(idx).
          zipWithIndex.
          map( arg => getParamPrimitive(arg._1, args(arg._2)))
        val hintList = 
          model.hintTypes(idx).
            zipWithIndex.
            map( arg => getParamPrimitive(arg._1, args(argList.length+arg._2)))
        (argList,hintList)  
    }catch {
      case t: Throwable => throw new Exception(s"BestGuessUDF Error Extracting Args and Hints: \n\tModel: ${model.name} \n\tArgs: [${args.mkString(",")}] \n\tSparkArgs: [${sparkArgs.mkString(",")}]", t)
    }
  }
  def varArgs(args:Any*):Any = {
    //TODO: Handle all params for spark udfs: ref @willsproth
    val (argList, hintList) = extractArgsAndHints(args)
    //logger.debug(s"-------------------------> UDF Param overflow: args:${argList.mkString("\n")} hints:${hintList.mkString("\n")}")
    getNative(model.bestGuess(idx, argList, hintList))
  }
  def getUDF = {
    val inFunc = sparkArgs.length match { 
      case 0 => () => {
        getNative(model.bestGuess(idx, Seq(), Seq()))
      }
      case 1 => (arg0:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 2 => (arg0:Any, arg1:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 3 => (arg0:Any, arg1:Any, arg2:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 4 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 5 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 6 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 7 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 8 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 9 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 10 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 11 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 12 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11 ))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 13 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 14 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 15 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 16 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 17 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 18 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 19 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 20 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 21 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any, arg20:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19, arg20))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case 22 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any, arg20:Any, arg21:Any) => {
        val (argList, hintList) = extractArgsAndHints(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19, arg20, arg21))
        getNative(model.bestGuess(idx, argList, hintList))
      }
      case x => varArgs _
    }
    ScalaUDF(
      inFunc,
      sparkVarType,
      if(sparkArgs.length > 22) Seq(CreateArray(sparkArgs)) else sparkArgs,
      org.apache.spark.sql.catalyst.ScalaReflection.getParameterTypeNullability(inFunc),
      if(sparkArgs.length > 22) Seq(ArrayType(StringType)) else sparkArgTypes,
      Some(model.name.id),true, true)
  }
}

case class SampleUDF(oper:Operator, model:Model, idx:Int, seed:Expression, args:Seq[Expression], hints:Seq[Expression]) extends MimirUDF {
  val sparkVarType = OperatorTranslation.getSparkType(model.varType(idx, model.argTypes(idx)))
  val sparkArgs = (args.map(arg => OperatorTranslation.mimirExprToSparkExpr(oper,arg)) ++ hints.map(hint => OperatorTranslation.mimirExprToSparkExpr(oper,hint))).toList.toSeq
  val sparkArgTypes = (model.argTypes(idx).map(arg => OperatorTranslation.getSparkType(arg)) ++ model.hintTypes(idx).map(hint => OperatorTranslation.getSparkType(hint))).toList.toSeq
  
  def extractArgsAndHintsSeed(args:Seq[Any]) : (Long, Seq[PrimitiveValue],Seq[PrimitiveValue]) ={
    try{
      val seedp = seed.toString().toLong
      val argList =
      model.argTypes(idx).
        zipWithIndex.
        map( arg => getPrimitive(arg._1, args(arg._2)))
      val hintList = 
        model.hintTypes(idx).
          zipWithIndex.
          map( arg => getPrimitive(arg._1, args(argList.length+arg._2)))
     (seedp, argList,hintList)
   }catch {
      case t: Throwable => throw new Exception(s"SampleUDF Error Extracting Args and Hints: \n\tModel: ${model.name} \n\tArgs: [${args.mkString(",")}] \n\tSparkArgs: [${sparkArgs.mkString(",")}]", t)
    }
  }
  def varArgs(args:Any*): Any = {
    //TODO: Handle all params for spark udfs: ref @willsproth
    val (seedi, argList, hintList) = extractArgsAndHintsSeed(args)
    getNative(model.sample(idx, seedi, argList, hintList))
  }
  def getUDF = {
    val inFunc = sparkArgs.length match { 
      case 0 => () => {
        getNative(model.sample(idx, 0, Seq(), Seq()))
      }
      case 1 => (arg0:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 2 => (arg0:Any, arg1:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 3 => (arg0:Any, arg1:Any, arg2:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 4 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 5 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 6 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 7 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 8 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 9 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 10 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 11 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 12 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11 ))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 13 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 14 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 15 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 16 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 17 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 18 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 19 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 20 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 21 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any, arg20:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19, arg20))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case 22 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any, arg20:Any, arg21:Any) => {
        val (seedi, argList, hintList) = extractArgsAndHintsSeed(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19, arg20, arg21))
        getNative(model.sample(idx, seedi, argList, hintList))
      }
      case x => varArgs _
    }
    ScalaUDF(
      inFunc,
      sparkVarType,
      if(sparkArgs.length > 22) Seq(CreateStruct(sparkArgs)) else sparkArgs,
      org.apache.spark.sql.catalyst.ScalaReflection.getParameterTypeNullability(inFunc),
      if(sparkArgs.length > 22) Seq(getStructType(sparkArgTypes)) else sparkArgTypes,
      Some(model.name.id), true, true)
  }
}

case class AckedUDF(oper:Operator, model:Model, idx:Int, args:Seq[Expression]) extends MimirUDF {
  val sparkArgs = (args.map(arg => OperatorTranslation.mimirExprToSparkExpr(oper,arg))).toList.toSeq
  val sparkArgTypes = (model.argTypes(idx).map(arg => OperatorTranslation.getSparkType(arg))).toList.toSeq
  def extractArgs(args:Seq[Any]) : Seq[PrimitiveValue] = {
    try{
      model.argTypes(idx).
        zipWithIndex.
        map( arg => getPrimitive(arg._1, args(arg._2)))
    }catch {
      case t: Throwable => throw new Exception(s"AckedUDF Error Extracting Args: \n\tModel: ${model.name} \n\tArgs: [${args.mkString(",")}] \n\tSparkArgs: [${sparkArgs.mkString(",")}]", t)
    }
  }
  def varArgs(args:Any*):Any = {
    //TODO: Handle all params for spark udfs: ref @willsproth
    val argList = extractArgs(args.toSeq)
    model.isAcknowledged(idx, argList)
  }
  def getUDF = {
    val inFunc = sparkArgs.length match { 
      case 0 => () => {
        new java.lang.Boolean(model.isAcknowledged(idx, Seq()))
      }
      case 1 => (arg0:Any) => {
        val argList = extractArgs(Seq(arg0))
        model.isAcknowledged(idx, argList)
      }
      case 2 => (arg0:Any, arg1:Any) => {
        val argList = extractArgs(Seq(arg0, arg1))
        model.isAcknowledged(idx, argList)
      }
      case 3 => (arg0:Any, arg1:Any, arg2:Any) => {
        val argList = extractArgs(Seq(arg0, arg1, arg2))
        model.isAcknowledged(idx, argList)
      }
      case 4 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any) => {
        val argList = extractArgs(Seq(arg0, arg1, arg2, arg3))
        model.isAcknowledged(idx, argList)
      }
      case 5 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any) => {
        val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4))
        model.isAcknowledged(idx, argList)
      }
      case 6 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any) => {
        val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5))
        model.isAcknowledged(idx, argList)
      }
      case 7 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any) => {
        val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6))
        model.isAcknowledged(idx, argList)
      }
      case 8 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any) => {
        val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7))
        model.isAcknowledged(idx, argList)
      }
      case 9 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any) => {
        val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8))
        model.isAcknowledged(idx, argList)
      }
      case 10 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9))
        model.isAcknowledged(idx, argList)
      }
      case 11 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10))
        model.isAcknowledged(idx, argList)
      }
      case 12 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11))
        model.isAcknowledged(idx, argList)
      }
      case 13 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12))
        model.isAcknowledged(idx, argList)
      }
      case 14 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13))
        model.isAcknowledged(idx, argList)
      }
      case 15 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14))
        model.isAcknowledged(idx, argList)
      }
      case 16 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15))
        model.isAcknowledged(idx, argList)
      }
      case 17 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16))
        model.isAcknowledged(idx, argList)
      }
      case 18 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17))
        model.isAcknowledged(idx, argList)
      }
      case 19 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18))
        model.isAcknowledged(idx, argList)
      }
      case 20 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19))
        model.isAcknowledged(idx, argList)
      }
      case 21 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any, arg20:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19, arg20))
        model.isAcknowledged(idx, argList)
      }
      case 22 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any, arg20:Any, arg21:Any) => {
        val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19, arg20, arg21))
        model.isAcknowledged(idx, argList)
      }
      case x => varArgs _
    }  
    ScalaUDF(
      inFunc,
      BooleanType,
      if(sparkArgs.length > 22) Seq(CreateStruct(sparkArgs)) else sparkArgs,
      org.apache.spark.sql.catalyst.ScalaReflection.getParameterTypeNullability(inFunc),
      if(sparkArgs.length > 22) Seq(getStructType(sparkArgTypes)) else sparkArgTypes,
      Some(model.name.id),true,true)
  }
}

case class FunctionUDF(oper:Operator, name:String, function:RegisteredFunction, params:Seq[Expression], argTypes:Seq[Type]) extends MimirUDF {
  val sparkArgs = (params.map(arg => OperatorTranslation.mimirExprToSparkExpr(oper,arg))).toList.toSeq
  val sparkArgTypes = argTypes.map(argT => OperatorTranslation.getSparkType(argT)).toList.toSeq
  val dataType = function match { 
    case NativeFunction(_, _, tc, _) => OperatorTranslation.getSparkType(tc(argTypes)) 
    case (_:ExpressionFunction | _:FoldFunction) => 
      throw new Exception(s"Unsupported function for Spark UDF: ${function}")
  }
  def extractArgs(args:Seq[Any]) : Seq[PrimitiveValue] = {
    try{
      argTypes.
        zipWithIndex.
        map( arg => getPrimitive(arg._1, args(arg._2)))
    }catch {
      case t: Throwable => throw new Exception(s"FunctionUDF Error Extracting Args: \n\tModel: ${name} \n\tArgs: [${args.mkString(",")}] \n\tSparkArgs: [${sparkArgs.mkString(",")}]", t)
    }
  }
  def varArgs(evaluator:Seq[PrimitiveValue] => PrimitiveValue)(args:Any*):Any = {
    //TODO: Handle all params for spark udfs: ref @willsproth
    val argList= extractArgs(args.toSeq)
    getNative(evaluator(argList))
  }
  def getUDF = {
    val inFunc = function match {
      case _:ExpressionFunction | _:FoldFunction => throw new Exception("Unsupported Function Type")
      case NativeFunction(_, evaluator, typechecker, _) => 
        sparkArgs.length match { 
          case 0 => () => {
            getNative(evaluator(Seq()))
          }
          case 1 => (arg0:Any) => {
            val argList = extractArgs(Seq(arg0))
            getNative(evaluator(argList))
          }
          case 2 => (arg0:Any, arg1:Any) => {
            val argList = extractArgs(Seq(arg0, arg1))
            getNative(evaluator(argList))
          }
          case 3 => (arg0:Any, arg1:Any, arg2:Any) => {
            val argList = extractArgs(Seq(arg0, arg1, arg2))
            getNative(evaluator(argList))
          }
          case 4 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any) => {
            val argList = extractArgs(Seq(arg0, arg1, arg2, arg3))
            getNative(evaluator(argList))
          }
          case 5 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any) => {
            val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4))
            getNative(evaluator(argList))
          }
          case 6 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any) => {
            val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5))
            getNative(evaluator(argList))
          }
          case 7 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any) => {
            val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6))
            getNative(evaluator(argList))
          }
          case 8 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any) => {
            val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7))
            getNative(evaluator(argList))
          }
          case 9 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any) => {
            val argList = extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8))
            getNative(evaluator(argList))
          }
          case 10 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9))
            getNative(evaluator(argList))
          }
          case 11 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10))
            getNative(evaluator(argList))
          }
          case 12 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11))
            getNative(evaluator(argList))
          }
          case 13 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12))
            getNative(evaluator(argList))
          }
          case 14 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13))
            getNative(evaluator(argList))
          }
          case 15 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14))
            getNative(evaluator(argList))
          }
          case 16 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15))
            getNative(evaluator(argList))
          }
          case 17 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16))
            getNative(evaluator(argList))
          }
          case 18 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17))
            getNative(evaluator(argList))
          }
          case 19 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18))
            getNative(evaluator(argList))
          }
          case 20 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19))
            getNative(evaluator(argList))
          }
          case 21 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any, arg20:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19, arg20))
            getNative(evaluator(argList))
          }
          case 22 => (arg0:Any, arg1:Any, arg2:Any, arg3:Any, arg4:Any, arg5:Any, arg6:Any, arg7:Any, arg8:Any, arg9:Any, arg10:Any, arg11:Any, arg12:Any, arg13:Any, arg14:Any, arg15:Any, arg16:Any, arg17:Any, arg18:Any, arg19:Any, arg20:Any, arg21:Any) => {
            val argList= extractArgs(Seq(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17, arg18, arg19, arg20, arg21))
            getNative(evaluator(argList))
          }
          case x => varArgs(evaluator) _ 
        }
      }
      ScalaUDF(
        inFunc,
        dataType,
        if(sparkArgs.length > 22) Seq(CreateStruct(sparkArgs)) else sparkArgs,
        org.apache.spark.sql.catalyst.ScalaReflection.getParameterTypeNullability(inFunc),
        if(sparkArgs.length > 22) Seq(getStructType(sparkArgTypes)) else sparkArgTypes,
        Some(name), true, true)
  }
}

case class GroupAnd(child: org.apache.spark.sql.catalyst.expressions.Expression) extends DeclarativeAggregate {
  override def children: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = child :: Nil
  override def nullable: Boolean = false
  // Return data type.
  override def dataType: DataType = BooleanType
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function group_and")
  private lazy val group_and = AttributeReference("group_and", BooleanType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = group_and :: Nil
  override lazy val initialValues: Seq[Literal] = Seq(
    Literal.create(true, BooleanType)
  )
  override lazy val updateExpressions: Seq[ org.apache.spark.sql.catalyst.expressions.Expression] = Seq(
    And(group_and, child)
  )
  override lazy val mergeExpressions: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = {
    Seq(
      And(group_and.left, group_and.right)
    )
  }
  override lazy val evaluateExpression: AttributeReference = group_and
}

case class GroupOr(child: org.apache.spark.sql.catalyst.expressions.Expression) extends DeclarativeAggregate {
  override def children: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = child :: Nil
  override def nullable: Boolean = false
  // Return data type.
  override def dataType: DataType = BooleanType
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function group_or")
  private lazy val group_or = AttributeReference("group_or", BooleanType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = group_or :: Nil
  override lazy val initialValues: Seq[Literal] = Seq(
    Literal.create(false, BooleanType)
  )
  override lazy val updateExpressions: Seq[ org.apache.spark.sql.catalyst.expressions.Expression] = Seq(
    Or(group_or, child)
  )
  override lazy val mergeExpressions: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = {
    Seq(
      Or(group_or.left, group_or.right)
    )
  }
  override lazy val evaluateExpression: AttributeReference = group_or
}

case class GroupBitwiseAnd(child: org.apache.spark.sql.catalyst.expressions.Expression) extends DeclarativeAggregate {
  override def children: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = child :: Nil
  override def nullable: Boolean = false
  // Return data type.
  override def dataType: DataType = LongType
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function group_bitwise_and")
  private lazy val group_bitwise_and = AttributeReference("group_bitwise_and", LongType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = group_bitwise_and :: Nil
  override lazy val initialValues: Seq[Literal] = Seq(
    Literal.create(0xffffffffffffffffl, LongType)
  )
  override lazy val updateExpressions: Seq[ org.apache.spark.sql.catalyst.expressions.Expression] = Seq(
    BitwiseAnd(group_bitwise_and, child)
  )
  override lazy val mergeExpressions: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = {
    Seq(
      BitwiseAnd(group_bitwise_and.left, group_bitwise_and.right)
    )
  }
  override lazy val evaluateExpression: AttributeReference = group_bitwise_and
}

case class GroupBitwiseOr(child: org.apache.spark.sql.catalyst.expressions.Expression) extends DeclarativeAggregate {
  override def children: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = child :: Nil
  override def nullable: Boolean = false
  // Return data type.
  override def dataType: DataType = LongType
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function group_bitwise_or")
  private lazy val group_bitwise_or = AttributeReference("group_bitwise_or", LongType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = group_bitwise_or :: Nil
  override lazy val initialValues: Seq[Literal] = Seq(
    Literal.create(0, LongType)
  )
  override lazy val updateExpressions: Seq[ org.apache.spark.sql.catalyst.expressions.Expression] = Seq(
    BitwiseOr(group_bitwise_or, child)
  )
  override lazy val mergeExpressions: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = {
    Seq(
      BitwiseOr(group_bitwise_or.left, group_bitwise_or.right)
    )
  }
  override lazy val evaluateExpression: AttributeReference = group_bitwise_or
}

case class JsonGroupArray(child: org.apache.spark.sql.catalyst.expressions.Expression) extends DeclarativeAggregate {
  override def children: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = child :: Nil
  override def nullable: Boolean = false
  // Return data type.
  override def dataType: DataType = StringType
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function json_group_array")
  private lazy val json_group_array = AttributeReference("json_group_array", StringType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = json_group_array :: Nil
  override lazy val initialValues: Seq[Literal] = Seq(
    Literal.create("", StringType)
  )
  override lazy val updateExpressions: Seq[ org.apache.spark.sql.catalyst.expressions.Expression] = Seq(
    If(IsNull(child),
      Concat(Seq(json_group_array, Literal(","), Literal("null"))),
      Concat(Seq(json_group_array, Literal(","), org.apache.spark.sql.catalyst.expressions.Cast(child,StringType,None)))) 
  )
  override lazy val mergeExpressions: Seq[org.apache.spark.sql.catalyst.expressions.Expression] = {
    Seq(
      Concat(Seq(json_group_array.left, json_group_array.right))
    )
  }
  override lazy val evaluateExpression = Concat(Seq(Literal("["), If(StartsWith(json_group_array,Literal(",")),Substring(json_group_array,Literal(2),Literal(Integer.MAX_VALUE)),json_group_array), Literal("]")))
}

