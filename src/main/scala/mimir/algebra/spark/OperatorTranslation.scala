package mimir.algebra.spark

import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.Dataset
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.{ArrayType, Metadata, DataType, DoubleType, LongType, FloatType, BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.catalyst.expressions.{RowNumber, MonotonicallyIncreasingID, NamedExpression, AttributeReference,Alias,SortOrder,Ascending,Descending,GreaterThanOrEqual,Literal, Add, Subtract, Multiply, Divide, BitwiseAnd, BitwiseOr, And, Or, ShiftLeft, ShiftRight, LessThan, LessThanOrEqual, GreaterThan, EqualTo, IsNull, Like, If, ScalaUDF, Rand, Randn}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.{TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogStorageFormat}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedRelation, UnresolvedInlineTable, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.plans.{JoinType, Inner,LeftOuter, Cross}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression,AggregateFunction,AggregateMode,Complete,Count,Average,Sum,First,Max,Min}


import org.apache.spark.sql.execution.datasources.{DataSource, FailureSafeParser}


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
import mimir.sql.sqlite.VGTermFunctions
import mimir.sql.sqlite.BestGuessVGTerm
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
import mimir.algebra.function.NativeFunction

import mimir.sql.SparkBackend
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.CreateStruct
import org.apache.spark.sql.catalyst.expressions.GetStructField
import org.apache.spark.sql.catalyst.plans.UsingJoin
import mimir.algebra.function.RegisteredFunction
import org.apache.spark.sql.catalyst.expressions.ConcatWs
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectList
import mimir.ctables.vgterm.Sampler

object OperatorTranslation
  extends LazyLogging
{
  var db:Database = null
  
  def mimirOpToSparkOp(oper:Operator) : LogicalPlan = {
    oper match {
      case Project(cols, src) => {
  			org.apache.spark.sql.catalyst.plans.logical.Project(cols.map(col => {
          mimirExprToSparkNamedExpr(oper, col.name, col.expression)
        }), mimirOpToSparkOp(src))
			}
			case ProvenanceOf(psel) => {
			  throw new Exception("Operator Translation not implemented '"+oper+"'")
			}
			case Annotate(subj,invisScm) => {
        throw new Exception("Operator Translation not implemented '"+oper+"'")
      }
			case Recover(subj,invisScm) => {
        throw new Exception("Operator Translation not implemented '"+oper+"'")
      }
			case Aggregate(groupBy, aggregates, source) => {
			  org.apache.spark.sql.catalyst.plans.logical.Aggregate(
          groupBy.map(mimirExprToSparkExpr(oper,_)),
          groupBy.map( gb => Alias( UnresolvedAttribute(gb.name), gb.name)()) ++ aggregates.map( mimirAggFunctionToSparkNamedExpr(oper,_)),
          mimirOpToSparkOp(source))
			}
			/*case Select(condition, Join(lhs, rhs)) => {
			  makeSparkJoin(lhs, rhs, Some(mimirExprToSparkExpr(oper,condition)), Inner) 
			}*/
			case Select(cond, src) => {
			  org.apache.spark.sql.catalyst.plans.logical.Filter(
			      mimirExprToSparkExpr(oper,cond), mimirOpToSparkOp(src))
			}
			case LeftOuterJoin(lhs, rhs, condition) => {
			  //org.apache.spark.sql.catalyst.plans.logical.Join( mimirOpToSparkOp(lhs),mimirOpToSparkOp(rhs), LeftOuter, Some(mimirExprToSparkExpr(oper,condition)))
			  makeSparkJoin(lhs, rhs, Some(mimirExprToSparkExpr(oper,condition)), LeftOuter) 
			}
			case Join(lhs, rhs) => {
			  //org.apache.spark.sql.catalyst.plans.logical.Join( mimirOpToSparkOp(lhs),mimirOpToSparkOp(rhs), Cross, None)
			  makeSparkJoin(lhs, rhs, None, Cross) 
			}
			case Union(lhs, rhs) => {
			  org.apache.spark.sql.catalyst.plans.logical.Union(mimirOpToSparkOp(lhs),mimirOpToSparkOp(rhs))
			}
			case Limit(offset, limit, query) => {
			  val limitOp = limit match {
			    case Some(limitI) => org.apache.spark.sql.catalyst.plans.logical.GlobalLimit(
			      Literal(limitI.toInt), 
			      mimirOpToSparkOp(query))
			    case None => mimirOpToSparkOp(query)
			  }
			  if(offset > 0){
			    logger.debug("------------------------------>Hacky Offset<-------------------------------------")
			    org.apache.spark.sql.catalyst.plans.logical.Filter(GreaterThanOrEqual(MonotonicallyIncreasingID(),Literal(offset)), limitOp)
			  }
			  else
			    limitOp
			}
			case Table(name, alias, sch, meta) => {
			  /*val table = CatalogTable(
          TableIdentifier(name),
          CatalogTableType.EXTERNAL,
          CatalogStorageFormat.empty,
          StructType(sch.map(col => StructField(col._1, getSparkType(col._2), true))) )*/
			  val tableId = TableIdentifier(name)
        val plan = UnresolvedRelation(tableId)
        val baseRelation  = if (!alias.isEmpty() && !alias.equals(name)) {
          SubqueryAlias(alias, plan)
        } else {
          plan
        }
			  meta match {
			    case Seq() => baseRelation
			    case _ => {
			      org.apache.spark.sql.catalyst.plans.logical.Project(sch.map(col => {
              mimirExprToSparkNamedExpr(oper, col._1, Var(col._1))
            }) ++ meta.filterNot(el => sch.unzip._1.contains(el._1)).distinct.map(metaEl => mimirExprToSparkNamedExpr(oper, metaEl._1, metaEl._2)) ,baseRelation)
			    }
			  } 
			  //LogicalRelation(baseRelation, table)
			}
			case View(name, query, annotations) => {
			  val schema = db.typechecker.schemaOf(query)
			  val table = CatalogTable(
          TableIdentifier(name),
          CatalogTableType.VIEW,
          CatalogStorageFormat.empty,
          mimirSchemaToStructType(schema) )
			  org.apache.spark.sql.catalyst.plans.logical.View( table,
          schema.map(col => {
            AttributeReference(col._1, getSparkType(col._2), true, Metadata.empty)( )
          }),
          mimirOpToSparkOp(query))
			}
      case av@AdaptiveView(schemaName, name, query, annotations) => {
        val schema = db.typechecker.schemaOf(av)
        val table = CatalogTable(
          TableIdentifier(name),
          CatalogTableType.VIEW,
          CatalogStorageFormat.empty,
          mimirSchemaToStructType(schema))
			  org.apache.spark.sql.catalyst.plans.logical.View( table,
          schema.map(col => {
            AttributeReference(col._1, getSparkType(col._2), true, Metadata.empty)( )
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
   db.backend.asInstanceOf[SparkBackend].sparkSql.sparkSession.sessionState.executePlan(plan) 
  }
  
  def dataset(plan:LogicalPlan) = {
    val qe = queryExecution(plan)
    qe.assertAnalyzed()
    new Dataset[Row](db.backend.asInstanceOf[SparkBackend].sparkSql.sparkSession, plan, RowEncoder(qe.analyzed.schema)) 
  }
  
  def makeInitSparkJoin(left: LogicalPlan, right: LogicalPlan, usingColumns: Seq[String], joinType: JoinType): LogicalPlan = {
    // Analyze the self join. The assumption is that the analyzer will disambiguate left vs right
    // by creating a new instance for one of the branch.
    val joined = db.backend.asInstanceOf[SparkBackend].sparkSql.sparkSession.sessionState.executePlan(
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
            tjoin.left.resolve(Seq(a.name), db.backend.asInstanceOf[SparkBackend].sparkSql.sparkSession.sessionState.analyzer.resolver).get,//p(withPlan(logicalPlan.left))('resolve)(a.name).asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression],
            tjoin.right.resolve(Seq(b.name), db.backend.asInstanceOf[SparkBackend].sparkSql.sparkSession.sessionState.analyzer.resolver).get)//p(withPlan(logicalPlan.right))('resolve)(b.name).asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression])
      }}
      tjoin.copy(condition = cond)
	  }
	  else   
  	  // Creates a Join node and resolve it first, to get join condition resolved, self-join resolved,
      // etc.
      //val joined = db.backend.asInstanceOf[SparkBackend].sparkSql.sparkSession.sessionState.executePlan(
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
                println(s"--------> output attr not resolved: ${b.name}")
                println(joined.right)
                joined.right.resolve(Seq(b.name), db.backend.asInstanceOf[SparkBackend].sparkSql.sparkSession.sessionState.analyzer.resolver).get
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
          logicalPlan.left.resolve(Seq(a.name), db.backend.asInstanceOf[SparkBackend].sparkSql.sparkSession.sessionState.analyzer.resolver).get,//p(withPlan(logicalPlan.left))('resolve)(a.name).asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression],
          logicalPlan.right.resolve(Seq(b.name), db.backend.asInstanceOf[SparkBackend].sparkSql.sparkSession.sessionState.analyzer.resolver).get)//p(withPlan(logicalPlan.right))('resolve)(b.name).asInstanceOf[org.apache.spark.sql.catalyst.expressions.Expression])
    }}

    logicalPlan.copy(condition = cond)
  }*/
  
  def getPrivateMamber[T](inst:AnyRef, fieldName:String) : T = {
    def _parents: Stream[Class[_]] = Stream(inst.getClass) #::: _parents.map(_.getSuperclass)
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
      println("=======================================")
      println(method)
      println(x)
      println(args)
      println("=======================================")
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
      println(someTrait)
      p(someTrait)('ofRows)(db.backend.asInstanceOf[SparkBackend].sparkSql.sparkSession, logicalPlan).asInstanceOf[DataFrame]
    
      //new Dataset[Row](db.backend.asInstanceOf[SparkBackend].sparkSql.sparkSession, logicalPlan, RowEncoder(logicalPlan.schema)).toDF()
    }
    catch {
      case t: Throwable => {
        println("-------------------------> Exception Executing Spark Op withPlan: ")
        println("------------------------ spark op --------------------------")
        println(logicalPlan)
        println("------------------------------------------------------------")
        throw t
      }
    }
  }
 
  
  def mimirExprToSparkNamedExpr(oper:Operator, name:String, expr:Expression) : NamedExpression = {
    Alias(mimirExprToSparkExpr(oper,expr),name)()
  }
  
  def mimirAggFunctionToSparkNamedExpr(oper:Operator, aggr:AggFunction) : NamedExpression = {
     Alias(AggregateExpression(aggr match {
         case AggFunction("COUNT", _, args, _) => {
           Count(args.map(mimirExprToSparkExpr(oper,_)))
         }
         case AggFunction("AVG", _, args, _) => {
           Average(mimirExprToSparkExpr(oper,args.head))
         }
         case AggFunction("SUM", _, args, _) => {
           Sum(mimirExprToSparkExpr(oper,args.head))
         }
         case AggFunction("FIRST", _, args, _) => {
           First(mimirExprToSparkExpr(oper,args.head),mimirExprToSparkExpr(oper,BoolPrimitive(true)))
         }
         case AggFunction("MAX", _, args, _) => {
           Max(mimirExprToSparkExpr(oper,args.head))
         }
         case AggFunction("MIN", _, args, _) => {
           Min(mimirExprToSparkExpr(oper,args.head))
         }
         case AggFunction("GROUP_AND", _, args, _) => {
           GroupAnd(mimirExprToSparkExpr(oper,args.head))
         }
         case AggFunction("GROUP_OR", _, args, _) => {
           GroupOr(mimirExprToSparkExpr(oper,args.head))
         }
         case AggFunction("GROUP_BITWISE_OR", _, args, _) => {
           GroupBitwiseOr(mimirExprToSparkExpr(oper,args.head))
         }
         case AggFunction("GROUP_BITWISE_AND", _, args, _) => {
           GroupBitwiseAnd(mimirExprToSparkExpr(oper,args.head))
         }
         case AggFunction("JSON_GROUP_ARRAY", _, args, _) => {
           JsonGroupArray(mimirExprToSparkExpr(oper,args.head))
           //TODO: when we add TArray() type we can use this
           //CollectList(mimirExprToSparkExpr(oper,args.head))
         }
         case AggFunction(function, distinct, args, alias) => {
           throw new Exception("Aggregate Function Translation not implemented '"+function+"'")
         } 
       },
       Complete,
       aggr.distinct
     ), aggr.alias)()
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
      case Var("ROWID") => {
        org.apache.spark.sql.catalyst.expressions.Cast(Alias(Add(MonotonicallyIncreasingID(), Literal(1)),"ROWID")(),getSparkType(db.backend.rowIdType),None)
      }
      /*case Var(name@Provenance.rowidColnameBase) => {
        org.apache.spark.sql.catalyst.expressions.Cast(Alias(Add(MonotonicallyIncreasingID(), Literal(1)),name)(),getSparkType(db.backend.rowIdType),None)
      }*/
      case Var(v) => {
        UnresolvedAttribute(v)
      }
      case rid@RowIdVar() => {
        //UnresolvedAttribute("ROWID")
        org.apache.spark.sql.catalyst.expressions.Cast(Alias(Add(MonotonicallyIncreasingID(), Literal(1)),rid.toString())(),getSparkType(db.backend.rowIdType),None)
      }
      case func@Function(_,_) => {
        mimirFunctionToSparkFunction(oper, func)
      }
      case BestGuess(model, idx, args, hints) => {
        val name = model.name
        //println(s"-------------------Translate BestGuess VGTerm($name, $idx, (${args.mkString(",")}), (${hints.mkString(",")}))")
       BestGuessUDF(oper, model, idx, args, hints).getUDF
        //UnresolvedFunction(mimir.ctables.CTables.FN_BEST_GUESS, mimirExprToSparkExpr(oper,StringPrimitive(name)) +: mimirExprToSparkExpr(oper,IntPrimitive(idx)) +: (args.map(mimirExprToSparkExpr(oper,_)) ++ hints.map(mimirExprToSparkExpr(oper,_))), true )
      }
      case IsAcknowledged(model, idx, args) => {
        val name = model.name
        //println(s"-------------------Translate IsAcknoledged VGTerm($name, $idx, (${args.mkString(",")}))")
        AckedUDF(oper, model, idx, args).getUDF
        //UnresolvedFunction(mimir.ctables.CTables.FN_IS_ACKED, mimirExprToSparkExpr(oper,StringPrimitive(name)) +: mimirExprToSparkExpr(oper,IntPrimitive(idx)) +: (args.map(mimirExprToSparkExpr(oper,_)) ), true )
      }
      case Sampler(model, idx, args, hints, seed) => {
        SampleUDF(oper, model, idx, seed, args, hints).getUDF
      }
      case VGTerm(name, idx, args, hints) => { //default to best guess
        //println(s"-------------------Translate VGTerm($name, $idx, (${args.mkString(",")}), (${hints.mkString(",")}))")
        val model = db.models.get(name)
        UnresolvedFunction(mimir.ctables.CTables.FN_BEST_GUESS, mimirExprToSparkExpr(oper,StringPrimitive(name)) +: mimirExprToSparkExpr(oper,IntPrimitive(idx)) +: (args.map(mimirExprToSparkExpr(oper,_)) ++ hints.map(mimirExprToSparkExpr(oper,_))), true )
      }
      case IsNullExpression(iexpr) => {
        IsNull(mimirExprToSparkExpr(oper,iexpr))
      }
      case Not(nexpr) => {
        org.apache.spark.sql.catalyst.expressions.Not(mimirExprToSparkExpr(oper,nexpr))
      }
      case x => {
        throw new Exception(s"Expression Translation not implemented ${x.getClass}: '$x'")
      }
    }
  }
  
  def mimirPrimitiveToSparkPrimitive(primitive : PrimitiveValue) : Literal = {
    primitive match {
      case NullPrimitive() => Literal(null)
      case RowIdPrimitive(s) => Literal(s)
      case StringPrimitive(s) => Literal(s)
      case IntPrimitive(i) => Literal(i)
      case FloatPrimitive(f) => Literal(f)
      case BoolPrimitive(b) => Literal(b)
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
      case x =>  UTF8String.fromString(x.asString)
    }
  }
  
  def mimirPrimitiveToSparkExternalRowValue(primitive : PrimitiveValue) : Any = {
    primitive match {
      case NullPrimitive() => null
      case RowIdPrimitive(s) => s
      case StringPrimitive(s) => s
      case IntPrimitive(i) => i
      case FloatPrimitive(f) => f
      case BoolPrimitive(b) => b
      case x =>  x.asString
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
    func match {
      case Function("CAST", params) => {
        org.apache.spark.sql.catalyst.expressions.Cast(mimirExprToSparkExpr(oper,params.head), getSparkType(params.tail.head.asInstanceOf[TypePrimitive].t), None)
      }
      case Function("MIMIR_CAST", params) => {
        throw new Exception(s"Function Translation not implemented MIMIR_CAST${params.mkString(",")}")
      }
      case Function("random", params) => {
        Randn(1L)
      }
      case Function(`vgtBGFunc`, params) => {
        throw new Exception(s"Function Translation not implemented $vgtBGFunc(${params.mkString(",")})")
      }
      case Function(name, params) => {
        FunctionUDF(oper, name, db.functions.get(name), params, params.map(arg => db.typechecker.typeOf(arg, oper))).getUDF
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
  
  def mimirSchemaToStructType(schema:Seq[(String, Type)]):StructType = {
    StructType(schema.map(col => StructField(col._1, getSparkType(col._2), true)))  
  }
  
  def structTypeToMimirSchema(schema:StructType): Seq[(String, Type)] = {
    schema.fields.map(col => (col.name, getMimirType(col.dataType)))  
  }
  
  def getSparkType(t:Type) : DataType = {
    t match {
      case TInt() => LongType
      case TFloat() => FloatType
      case TDate() => StringType
      case TString() => StringType
      case TBool() => BooleanType
      case TRowId() => StringType
      case TType() => StringType
      case TAny() => StringType
      case TTimestamp() => StringType
      case TInterval() => StringType
      case TUser(name) => getSparkType(mimir.algebra.TypeRegistry.registeredTypes(name)._2)
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
      case _ => TString()
    }
  }
  
  def extractTables(oper: Operator): Seq[String] = 
  {
    oper match {
      case Table(name, alias, tgtSch, tgtMetadata) => Seq(name)
      case _ => oper.children.map(extractTables(_)).flatten
    }
  }
  
  /*def mimirOpToDF(sqlContext:SQLContext, oper:Operator) : DataFrame = {
    val sparkOper = OperatorTranslation.mimirOpToSparkOp(oper)
    println("---------------------------- Mimir Oper -----------------------------")
    println(oper)
    println("---------------------------- Spark Oper -----------------------------")
    println(sparkOper)
    println("---------------------------------------------------------------------")
    
    val sparkTables = sqlContext.sparkSession.catalog.listTables().collect()
    extractTables(oper).map(t => {
      if(!sparkTables.contains(t)){
        println(s"loading table into spark: $t")
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
    println(cls.getName)
    sqlContext.sparkSession.baseRelationToDataFrame(
      DataSource.apply(
        sqlContext.sparkSession,
        paths = Seq.empty,
        userSpecifiedSchema = None,
        className = "jdbc",
        options = Map("url" -> "jdbc:sqlite:debug.db") ).resolveRelation())*/
  }*/
  
}

class MimirUDF {
  def getPrimitive(t:Type, value:Any) = value match {
    case null => NullPrimitive()
    case _ => t match {
      //case TInt() => IntPrimitive(value.asInstanceOf[Long])
      case TInt() => IntPrimitive(value.asInstanceOf[Long])
      case TFloat() => FloatPrimitive(value.asInstanceOf[Float].toDouble)
      //case TDate() => DatePrimitive.(value.asInstanceOf[Long])
      //case TTimestamp() => Primitive(value.asInstanceOf[Long])
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
      case FloatPrimitive(f) => new java.lang.Float(f)
      case BoolPrimitive(b) => new java.lang.Boolean(b)
      case x =>  x.asString
    }
}


case class BestGuessUDF(oper:Operator, model:Model, idx:Int, args:Seq[Expression], hints:Seq[Expression]) extends MimirUDF {
  val sparkVarType = OperatorTranslation.getSparkType(model.varType(idx, model.argTypes(idx)))
  val sparkArgs = (args.map(arg => OperatorTranslation.mimirExprToSparkExpr(oper,arg)) ++ hints.map(hint => OperatorTranslation.mimirExprToSparkExpr(oper,hint))).toList.toSeq
  val sparkArgTypes = (model.argTypes(idx).map(arg => OperatorTranslation.getSparkType(arg)) ++ model.hintTypes(idx).map(hint => OperatorTranslation.getSparkType(hint))).toList.toSeq
  
  def extractArgsAndHints(args:Seq[Any]) : (Seq[PrimitiveValue],Seq[PrimitiveValue]) ={
    val argList =
    model.argTypes(idx).
      zipWithIndex.
      map( arg => getPrimitive(arg._1, args(arg._2)))
    val hintList = 
      model.hintTypes(idx).
        zipWithIndex.
        map( arg => getPrimitive(arg._1, args(argList.length+arg._2)))
    (argList,hintList)  
  }
  def getUDF = 
    ScalaUDF(
      sparkArgs.length match { 
        case 0 => {
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
      },
      sparkVarType,
      sparkArgs,
      sparkArgTypes,
      Some(model.name))
}

case class SampleUDF(oper:Operator, model:Model, idx:Int, seed:Expression, args:Seq[Expression], hints:Seq[Expression]) extends MimirUDF {
  val sparkVarType = OperatorTranslation.getSparkType(model.varType(idx, model.argTypes(idx)))
  val sparkArgs = (args.map(arg => OperatorTranslation.mimirExprToSparkExpr(oper,arg)) ++ hints.map(hint => OperatorTranslation.mimirExprToSparkExpr(oper,hint))).toList.toSeq
  val sparkArgTypes = (model.argTypes(idx).map(arg => OperatorTranslation.getSparkType(arg)) ++ model.hintTypes(idx).map(hint => OperatorTranslation.getSparkType(hint))).toList.toSeq
  
  def extractArgsAndHintsSeed(args:Seq[Any]) : (Long, Seq[PrimitiveValue],Seq[PrimitiveValue]) ={
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
  }
  def getUDF = 
    ScalaUDF(
      sparkArgs.length match { 
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
      },
      sparkVarType,
      sparkArgs,
      sparkArgTypes,
      Some(model.name))
}

case class AckedUDF(oper:Operator, model:Model, idx:Int, args:Seq[Expression]) extends MimirUDF {
  val sparkArgs = (args.map(arg => OperatorTranslation.mimirExprToSparkExpr(oper,arg))).toList.toSeq
  val sparkArgTypes = (model.argTypes(idx).map(arg => OperatorTranslation.getSparkType(arg))).toList.toSeq
  def extractArgs(args:Seq[Any]) : Seq[PrimitiveValue] = {
    model.argTypes(idx).
      zipWithIndex.
      map( arg => getPrimitive(arg._1, args(arg._2)))
  }
  def getUDF = 
    ScalaUDF(
      sparkArgs.length match { 
        case 0 => {
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
      },
      BooleanType,
      sparkArgs,
      sparkArgTypes,
      Some(model.name))
}

case class FunctionUDF(oper:Operator, name:String, function:RegisteredFunction, params:Seq[Expression], argTypes:Seq[Type]) extends MimirUDF {
  val sparkArgs = (params.map(arg => OperatorTranslation.mimirExprToSparkExpr(oper,arg))).toList.toSeq
  val sparkArgTypes = argTypes.map(argT => OperatorTranslation.getSparkType(argT)).toList.toSeq
  val dataType = function match { case NativeFunction(_, _, tc, _) => OperatorTranslation.getSparkType(tc(argTypes)) }
  def extractArgs(args:Seq[Any]) : Seq[PrimitiveValue] = {
    argTypes.
      zipWithIndex.
      map( arg => getPrimitive(arg._1, args(arg._2)))
  }
  def getUDF = 
    ScalaUDF(
      function match {
        case NativeFunction(_, evaluator, typechecker, _) => 
          sparkArgs.length match { 
            case 0 => {
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
          }
      },
      dataType,
      sparkArgs,
      sparkArgTypes,
      Some(name))
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