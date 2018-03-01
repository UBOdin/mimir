package mimir.algebra.spark

import org.apache.spark.sql.{SQLContext, DataFrame, Row, Dataset}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.{Metadata, DataType, DoubleType, LongType, FloatType, BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.catalyst.expressions.{MonotonicallyIncreasingID, NamedExpression, AttributeReference,Alias,SortOrder,Ascending,Descending,GreaterThanOrEqual,Literal, Add, Subtract, Multiply, Divide, BitwiseAnd, BitwiseOr, And, Or, LessThan, LessThanOrEqual, GreaterThan, EqualTo, IsNull, Like, If, ScalaUDF, Rand, Randn}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.{TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogStorageFormat}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedRelation, UnresolvedInlineTable, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.plans.{JoinType, Inner,LeftOuter}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression,AggregateFunction,AggregateMode,Complete,Count,Average,Sum,First,Max,Min}


import org.apache.spark.sql.execution.datasources.{DataSource, FailureSafeParser}


import mimir.algebra._
import mimir.Database
import org.apache.spark.sql.types._
import mimir.provenance.Provenance

object OperatorTranslation {
  var db:Database = null
  
  def mimirOpToSparkOp(oper:Operator) : LogicalPlan = {
    oper match {
      case Project(cols, src) => {
  			org.apache.spark.sql.catalyst.plans.logical.Project(cols.map(col => {
          mimirExprToSparkNamedExpr(col.name, col.expression)
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
          groupBy.map(mimirExprToSparkExpr(_)),
          aggregates.map(mimirAggFunctionToSparkNamedExpr(_)),
          mimirOpToSparkOp(source))
			}
			case Select(cond, src) => {
			  org.apache.spark.sql.catalyst.plans.logical.Filter(
			      mimirExprToSparkExpr(cond), mimirOpToSparkOp(src))
			}
			case LeftOuterJoin(lhs, rhs, condition) => {
			  org.apache.spark.sql.catalyst.plans.logical.Join( mimirOpToSparkOp(lhs),mimirOpToSparkOp(rhs), LeftOuter, Some(mimirExprToSparkExpr(condition)))
			}
			case Join(lhs, rhs) => {
			  org.apache.spark.sql.catalyst.plans.logical.Join( mimirOpToSparkOp(lhs),mimirOpToSparkOp(rhs), Inner, None)
			}
			case Union(lhs, rhs) => {
			  org.apache.spark.sql.catalyst.plans.logical.Union(mimirOpToSparkOp(lhs),mimirOpToSparkOp(rhs))
			}
			case Limit(offset, limit, query) => {
			  limit match {
			    case Some(limitI) => org.apache.spark.sql.catalyst.plans.logical.GlobalLimit(
			      Literal(limitI.toInt), 
			      mimirOpToSparkOp(query))
			    case None => mimirOpToSparkOp(query)
			  }
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
			  baseRelation
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
        UnresolvedInlineTable( schema.unzip._1, data.map(row => row.map(mimirExprToSparkExpr(_))))
      }
			case Sort(sortCols, src) => {
			  org.apache.spark.sql.catalyst.plans.logical.Sort(
          sortCols.map(sortCol => SortOrder(mimirExprToSparkExpr(sortCol.expression),if(sortCol.ascending) Ascending else Descending)),
          true,
          mimirOpToSparkOp(src))
			}
			case _ => {
			  throw new Exception("Operator Translation not implemented '"+oper+"'")
			}
    }
  }
  
  def mimirExprToSparkNamedExpr(name:String, expr:Expression) : NamedExpression = {
    Alias(mimirExprToSparkExpr(expr),name)()
  }
  
  def mimirAggFunctionToSparkNamedExpr(aggr:AggFunction) : NamedExpression = {
     Alias(AggregateExpression(aggr match {
         case AggFunction("COUNT", _, args, _) => {
           Count(args.map(mimirExprToSparkExpr(_)))
         }
         case AggFunction("AVG", _, args, _) => {
           Average(mimirExprToSparkExpr(args.head))
         }
         case AggFunction("SUM", _, args, _) => {
           Sum(mimirExprToSparkExpr(args.head))
         }
         case AggFunction("FIRST", _, args, _) => {
           First(mimirExprToSparkExpr(args.head),mimirExprToSparkExpr(BoolPrimitive(false)))
         }
         case AggFunction("MAX", _, args, _) => {
           Max(mimirExprToSparkExpr(args.head))
         }
         case AggFunction("MIN", _, args, _) => {
           Min(mimirExprToSparkExpr(args.head))
         }
         case AggFunction(function, distinct, args, alias) => {
           throw new Exception("Aggregate Function Translation not implemented '"+function+"'")
         } 
       },
       Complete,
       aggr.distinct
     ), aggr.alias)()
  }
  
  def mimirExprToSparkExpr(expr:Expression) : org.apache.spark.sql.catalyst.expressions.Expression = {
    expr match {
      case primitive : PrimitiveValue => {
        mimirPrimitiveToSparkPrimitive(primitive)
      }
      case cmp@Comparison(op,lhs,rhs) => {
        mimirComparisonToSparkComparison(cmp)
      }
      case arith@Arithmetic(op,lhs,rhs) => {
        mimirArithmeticToSparkArithmetic(arith)
      }
      case cnd@Conditional(condition,thenClause,elseClause) => {
        mimirConditionalToSparkConditional(cnd)
      }
      case Var(name@Provenance.rowidColnameBase) => {
        org.apache.spark.sql.catalyst.expressions.Cast(Alias(MonotonicallyIncreasingID(),name)(),getSparkType(db.backend.rowIdType),None)
      }
      case Var(v) => {
        UnresolvedAttribute(v)
      }
      case RowIdVar() => {
        UnresolvedAttribute("ROWID")
      }
      case func@Function(_,_) => {
        mimirFunctionToSparkFunction(func)
      }
      case VGTerm(name, idx, args, hints) => {
        val model = db.models.get(name)
        /*ScalaUDF(
          model.bestGuess(idx,Seq(),Seq()),
          getSparkType(model.varType(idx, model.argTypes(idx))),
          args.map(mimirExprToSparkExpr(_)),
          model.hintTypes(idx).map(getSparkType(_)),
          Some(name))*/
        UnresolvedFunction(mimir.ctables.CTables.FN_BEST_GUESS, mimirExprToSparkExpr(StringPrimitive(name)) +: mimirExprToSparkExpr(IntPrimitive(idx)) +: (args.map(mimirExprToSparkExpr(_)) ++ hints.map(mimirExprToSparkExpr(_))), true )
      }
      case IsNullExpression(iexpr) => {
        IsNull(mimirExprToSparkExpr(iexpr))
      }
      case Not(nexpr) => {
        org.apache.spark.sql.catalyst.expressions.Not(mimirExprToSparkExpr(nexpr))
      }
      case x => {
        throw new Exception("Expression Translation not implemented '"+x+"'")
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
      case x =>  Literal(x.asString)
    }
  }
  
  def mimirComparisonToSparkComparison(cmp:Comparison) : org.apache.spark.sql.catalyst.expressions.Expression = {
    cmp.op match {
      case  Cmp.Eq => EqualTo(mimirExprToSparkExpr(cmp.lhs), mimirExprToSparkExpr(cmp.rhs))
      case  Cmp.Neq  => org.apache.spark.sql.catalyst.expressions.Not(EqualTo(mimirExprToSparkExpr(cmp.lhs), mimirExprToSparkExpr(cmp.rhs))) 
      case  Cmp.Gt  => GreaterThan(mimirExprToSparkExpr(cmp.lhs), mimirExprToSparkExpr(cmp.rhs))
      case  Cmp.Gte  => GreaterThanOrEqual(mimirExprToSparkExpr(cmp.lhs), mimirExprToSparkExpr(cmp.rhs))
      case  Cmp.Lt  => LessThan(mimirExprToSparkExpr(cmp.lhs), mimirExprToSparkExpr(cmp.rhs))
      case  Cmp.Lte  => LessThanOrEqual(mimirExprToSparkExpr(cmp.lhs), mimirExprToSparkExpr(cmp.rhs))
      case  Cmp.Like  => Like(mimirExprToSparkExpr(cmp.lhs), mimirExprToSparkExpr(cmp.rhs)) 
      case  Cmp.NotLike => org.apache.spark.sql.catalyst.expressions.Not(Like(mimirExprToSparkExpr(cmp.lhs), mimirExprToSparkExpr(cmp.rhs)))
      case x => throw new Exception("Invalid operand '"+x+"'")
    }
  }
  
  def mimirArithmeticToSparkArithmetic(arith:Arithmetic) : org.apache.spark.sql.catalyst.expressions.Expression = {
    arith.op match {
      case  Arith.Add => Add(mimirExprToSparkExpr(arith.lhs),mimirExprToSparkExpr(arith.rhs)) 
      case  Arith.Sub => Subtract(mimirExprToSparkExpr(arith.lhs),mimirExprToSparkExpr(arith.rhs)) 
      case  Arith.Mult => Multiply(mimirExprToSparkExpr(arith.lhs),mimirExprToSparkExpr(arith.rhs)) 
      case  Arith.Div => Divide(mimirExprToSparkExpr(arith.lhs),mimirExprToSparkExpr(arith.rhs))  
      case  Arith.BitAnd => BitwiseAnd(mimirExprToSparkExpr(arith.lhs),mimirExprToSparkExpr(arith.rhs))  
      case  Arith.BitOr => BitwiseOr(mimirExprToSparkExpr(arith.lhs),mimirExprToSparkExpr(arith.rhs))  
      case  Arith.And => And(mimirExprToSparkExpr(arith.lhs),mimirExprToSparkExpr(arith.rhs))   
      case  Arith.Or => Or(mimirExprToSparkExpr(arith.lhs),mimirExprToSparkExpr(arith.rhs))   
      case x => throw new Exception("Invalid operand '"+x+"'")
    }
  }
  
  def mimirConditionalToSparkConditional(cnd:Conditional) : org.apache.spark.sql.catalyst.expressions.Expression = {
    cnd match {
      case Conditional(cond, thenClause, elseClause) => {
        If(mimirExprToSparkExpr(cond),mimirExprToSparkExpr(thenClause),mimirExprToSparkExpr(elseClause))
      }
    }
  }
  
  def mimirFunctionToSparkFunction(func:Function) : org.apache.spark.sql.catalyst.expressions.Expression = {
    func match {
      case Function("CAST", params) => {
        org.apache.spark.sql.catalyst.expressions.Cast(mimirExprToSparkExpr(params.head), getSparkType(params.tail.head.asInstanceOf[TypePrimitive].t), None)
      }
      case Function("MIMIR_CAST", params) => {
        throw new Exception(s"Function Translation not implemented MIMIR_CAST${params.mkString(",")}")
      }
      case Function("random", params) => {
        Randn(1L)
      }
      case Function(name, params) => {
        throw new Exception(s"Function Translation not implemented $name${params.mkString(",")}")
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
      case "INT" => IntegerType
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
      case TInt() => StringType
      case TFloat() => StringType
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
      case LongType => TFloat()
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