package mimir.exec.spark

import org.apache.spark.sql.{SQLContext, DataFrame, Row, SparkSession}
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
import java.sql.SQLException

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
import mimir.exec.spark.udf._
import mimir.exec.sqlite.VGTermFunctions
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.expressions.aggregate.StddevSamp
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.CreateArray
import org.apache.spark.sql.types.TypeCollection

class RAToSpark(db: mimir.Database)
  extends LazyLogging
{
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
			case Table(name, alias, source, sch, meta) => {
			  /*val table = CatalogTable(
          TableIdentifier(name),
          CatalogTableType.EXTERNAL,
          CatalogStorageFormat.empty,
          StructType(sch.map(col => StructField(col._1, getSparkType(col._2), true))) )*/
			  // val tableId = TableIdentifier(name.id)
        //we can do this to compute the rowid inline

        val provider = db.catalog.getSchemaProvider(source)

        provider.logicalplan(name) match {
          case Some(plan) => {
    			  val realSchema = provider.tableSchema(name).get
            val relationWithRowIDs =
              RowIndexPlan(plan, realSchema).getPlan(db)
    			  val baseRelation  = 
              if (!alias.equals(name)) {
                SubqueryAlias(alias.id, relationWithRowIDs)
              } else { relationWithRowIDs }

              //here we check if the real table schema matches the table op schema 
              // because the table op schema may have been rewritten by deepRenameColumn
              // - like when there is a join with conflicts
              val attributesNeedingProjection: Map[ID, ID] = 
                sch.zip(realSchema).flatMap {
                  case (tableSchEl, realSchEl) => {
                    if(tableSchEl._1.equals(realSchEl._1))
                      None
                    else
                      Some((tableSchEl._1,realSchEl._1))
                  }
                }.toMap[ID, ID]

              if(attributesNeedingProjection.isEmpty) {
                meta match {
                  case Seq() => baseRelation
                  case _ => {
                    org.apache.spark.sql.catalyst.plans.logical.Project(sch.map(col => {
                      mimirExprToSparkNamedExpr(oper, col._1, Var(col._1))
                    }) ++ meta.filterNot(el => sch.unzip._1.contains(el._1)).distinct.map(metaEl => mimirExprToSparkNamedExpr(oper, metaEl._1, metaEl._2)) ,baseRelation)
                  }
                }
              } else {
                org.apache.spark.sql.catalyst.plans.logical.Project(
                  oper.columnNames.filterNot(el => meta.unzip3._1.contains(el)).map { col =>
                    attributesNeedingProjection.get(col) match {  
                      case Some(target) => 
                        mimirExprToSparkNamedExpr(oper, col, Var(target)) 
                      case None => mimirExprToSparkNamedExpr(oper, col, Var(col)) 
                      }
                  } ++ meta.filterNot(el => sch.unzip._1.contains(el._1)).filterNot(el => realSchema.unzip._1.contains(el._1)).distinct.map(metaEl => mimirExprToSparkNamedExpr(oper, metaEl._1, metaEl._2)),
                  baseRelation
                )
              }

          }

          // If we're here, the provider doesn't define the table inline, and
          // instead uses a view. In principle, all views should have gotten 
          // expanded by now, but we can still handle this case by expanding 
          // out recursively.
          case None => 
            provider.view(name) match {
              case Some(view) => mimirOpToSparkOp(view)
              case None => 
                throw new SQLException(s"Invalid table: $source.$name")
            }

        }

			}
			case View(name, query, annotations) => {
			  val schema = db.typechecker.schemaOf(query)
			  val table = CatalogTable(
          TableIdentifier(name.id),
          CatalogTableType.VIEW,
          CatalogStorageFormat.empty,
          RAToSpark.mimirSchemaToStructType(schema) )
        org.apache.spark.sql.catalyst.plans.logical.View( table,
          schema.map(col => {
            AttributeReference(col._1.id, RAToSpark.getSparkType(col._2), true, Metadata.empty)( )
          }),
          mimirOpToSparkOp(query))
			}
      case av@AdaptiveView(schemaName, name, query, annotations) => {
        val schema = db.typechecker.schemaOf(av)
        val table = CatalogTable(
          TableIdentifier(name.id),
          CatalogTableType.VIEW,
          CatalogStorageFormat.empty,
          RAToSpark.mimirSchemaToStructType(schema))
			  org.apache.spark.sql.catalyst.plans.logical.View( table,
          schema.map(col => {
            AttributeReference(col._1.id, RAToSpark.getSparkType(col._2), true, Metadata.empty)( )
          }),
          mimirOpToSparkOp(query))
      }
      case HardTable(schema, data)=> {
        //UnresolvedInlineTable( schema.unzip._1, data.map(row => row.map(mimirExprToSparkExpr(oper,_))))
        /*LocalRelation(mimirSchemaToStructType(schema).map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()), 
            data.map(row => InternalRow(row.map(mimirPrimitiveToSparkInternalRowValue(_)):_*)))*/
        LocalRelation.fromExternalRows(RAToSpark.mimirSchemaToStructType(schema).map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()), 
            data.map(row => Row(row.map(RAToSpark.mimirPrimitiveToSparkExternalRowValue(_)):_*)))     
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
    MimirSpark.get.sparkSession.sessionState.executePlan(plan)
  } 
  
  def makeInitSparkJoin(left: LogicalPlan, right: LogicalPlan, usingColumns: Seq[String], joinType: JoinType): LogicalPlan = {
    // Analyze the self join. The assumption is that the analyzer will disambiguate left vs right
    // by creating a new instance for one of the branch.
    val joined = MimirSpark.get.sparkSession.sessionState.executePlan(
       org.apache.spark.sql.catalyst.plans.logical.Join(left, right, joinType = joinType, None))
      .analyzed.asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Join]

    org.apache.spark.sql.catalyst.plans.logical.Join(
        joined.left,
        joined.right,
        UsingJoin(joinType, usingColumns),
        None)
    
  }


  def dataset(plan:LogicalPlan) = {
    val qe = queryExecution(plan)
    qe.assertAnalyzed()
    val sparkSession = MimirSpark.get
    new Dataset[Row](sparkSession, plan, RowEncoder(qe.analyzed.schema)) 
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
            tjoin.left.resolve(Seq(a.name), MimirSpark.get.sparkSession.sessionState.analyzer.resolver).get,//p(withPlan(logicalPlan.left))('resolve)(a.name).asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression],
            tjoin.right.resolve(Seq(b.name), MimirSpark.get.sparkSession.sessionState.analyzer.resolver).get)//p(withPlan(logicalPlan.right))('resolve)(b.name).asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression])
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
      p(someTrait)('ofRows)(MimirSpark.get.sparkSession, logicalPlan).asInstanceOf[DataFrame]
    
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
          val fi = MimirSpark.get.sparkSession.sessionState.catalog.lookupFunctionInfo(FunctionIdentifier(function.toLowerCase()))
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
        RAToSpark.mimirPrimitiveToSparkPrimitive(primitive)
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
        UnresolvedAttribute.quoted(name.id)
      }
      case Var(v) => {
        UnresolvedAttribute.quoted(v.id)
      }
      case rid@RowIdVar() => {
        UnresolvedAttribute("ROWID")
      }
      case func@Function(_,_) => {
        mimirFunctionToSparkFunction(oper, func)
      }
      case CastExpression(expr, t) => {
        org.apache.spark.sql.catalyst.expressions.Cast(mimirExprToSparkExpr(oper,expr), RAToSpark.getSparkType(t), None)
      }
      case BestGuess(model, idx, args, hints) => {
        val name = model.name
        //logger.debug(s"-------------------Translate BestGuess VGTerm($name, $idx, (${args.mkString(",")}), (${hints.mkString(",")}))")
       BestGuessUDF(oper, model, idx, args.map(arg => mimirExprToSparkExpr(oper,arg)), hints.map(hint => mimirExprToSparkExpr(oper,hint))).getUDF
        //UnresolvedFunction(mimir.ctables.CTables.FN_BEST_GUESS, mimirExprToSparkExpr(oper,StringPrimitive(name)) +: mimirExprToSparkExpr(oper,IntPrimitive(idx)) +: (args.map(mimirExprToSparkExpr(oper,_)) ++ hints.map(mimirExprToSparkExpr(oper,_))), true )
      }
      case IsAcknowledged(model, idx, args) => {
        val name = model.name
        //logger.debug(s"-------------------Translate IsAcknoledged VGTerm($name, $idx, (${args.mkString(",")}))")
        AckedUDF(oper, model, idx, args.map(arg => mimirExprToSparkExpr(oper,arg))).getUDF
        //UnresolvedFunction(mimir.ctables.CTables.FN_IS_ACKED, mimirExprToSparkExpr(oper,StringPrimitive(name)) +: mimirExprToSparkExpr(oper,IntPrimitive(idx)) +: (args.map(mimirExprToSparkExpr(oper,_)) ), true )
      }
      case Sampler(model, idx, args, hints, seed) => {
        SampleUDF(oper, model, idx, seed, args.map(arg => mimirExprToSparkExpr(oper,arg)), hints.map(hint => mimirExprToSparkExpr(oper,hint))).getUDF
      }
      case VGTerm(name, idx, args, hints) => { //default to best guess
        //logger.debug(s"-------------------Translate VGTerm($name, $idx, (${args.mkString(",")}), (${hints.mkString(",")}))")
        val model = db.models.get(name)
        BestGuessUDF(oper, model, idx, args.map(arg => mimirExprToSparkExpr(oper,arg)), hints.map(hint => mimirExprToSparkExpr(oper,hint))).getUDF
        //UnresolvedFunction(mimir.ctables.CTables.FN_BEST_GUESS, mimirExprToSparkExpr(oper,StringPrimitive(name)) +: mimirExprToSparkExpr(oper,IntPrimitive(idx)) +: (args.map(mimirExprToSparkExpr(oper,_)) ++ hints.map(mimirExprToSparkExpr(oper,_))), true )
      }
      case DataWarning(_, v, _, _, _) => {
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
        FunctionUDF(oper, name, db.functions.get(func.op), func.params.map(arg => mimirExprToSparkExpr(oper,arg)), func.params.map(arg => db.typechecker.typeOf(arg, oper))).getUDF
      }
    }
  }
}

object RAToSpark {

  val defaultDate = DateTimeUtils.toJavaDate(0)
  val defaultTimestamp = DateTimeUtils.toJavaTimestamp(0L)

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
  
  def structTypeToMimirSchema(schema:StructType): Seq[(ID, Type)] = {
    schema.fields.map(col => (ID(col.name), getMimirType(col.dataType)))  
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
        case TDate() => this.defaultDate
        case TString() => ""
        case TBool() => new java.lang.Boolean(false)
        case TRowId() => ""
        case TType() => ""
        case TAny() => ""
        case TTimestamp() => this.defaultTimestamp
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
      case Table(name, alias, source, tgtSch, tgtMetadata) => Seq(name.id)
      case _ => oper.children.map(extractTables(_)).flatten
    }
  }
  
  /*def mimirOpToDF(sqlContext:SQLContext, oper:Operator) : DataFrame = {
    val sparkOper = RAToSpark.mimirOpToSparkOp(oper)
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
    new Dataset[Row](sqlContext.sparkSession, RAToSpark.mimirOpToSparkOp(oper), RowEncoder(qe.analyzed.schema)).toDF()
    
        
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
  
}



