package mimir.ctables;
 
import java.util.Random;
import com.typesafe.scalalogging.LazyLogging

import mimir._
import mimir.algebra._
import mimir.provenance._
import mimir.exec._
import mimir.exec.mode._
import mimir.util._
import mimir.optimizer.operator._
import mimir.models._
import mimir.views.ViewManager

case class InvalidProvenance(msg: String, token: RowIdPrimitive) 
	extends Exception("Invalid Provenance Token ["+msg+"]: "+token);

abstract class Explanation(
	val reasons: List[Reason], 
	val token: RowIdPrimitive
) {
	def fields: List[(String, PrimitiveValue)]

	override def toString(): String = { 
		(fields ++ List( 
			("Reasons", reasons),
			("Token", token.v)
		)).map((x) => x._1+": "+x._2).mkString("\n")
	}

	def toJSON(): String = {
		JSONBuilder.dict(
			fields ++ 
			List(
				("reasons", 
					reasons.map( _.toJSON ) ),
				("token", token.v)
		))
	}
}

case class RowExplanation (
	val probability: Double, 
	override val reasons: List[Reason], 
	override val token: RowIdPrimitive
) extends Explanation(reasons, token) {
	def fields = List(
		("probability", FloatPrimitive(probability))
	)
}

class CellExplanation(
	val examples: List[PrimitiveValue],
	override val reasons: List[Reason], 
	override val token: RowIdPrimitive,
	val column: ID
) extends Explanation(reasons, token) {
	def fields = List[(String,PrimitiveValue)](
		("examples", StringPrimitive(examples.map( _.toString ).mkString(", "))),
		("column", StringPrimitive(column.id))
	)
}

case class GenericCellExplanation (
	override val examples: List[PrimitiveValue],
	override val reasons: List[Reason], 
	override val token: RowIdPrimitive,
	override val column: ID
) extends CellExplanation(examples, reasons, token, column) {
}

case class NumericCellExplanation (
	val mean: PrimitiveValue,
	val sttdev: PrimitiveValue,
	override val examples: List[PrimitiveValue],
	override val reasons: List[Reason], 
	override val token: RowIdPrimitive,
	override val column: ID
) extends CellExplanation(examples, reasons, token, column) {
	override def fields = 
		List(
			("mean", mean),
			("stddev", sttdev)
		) ++ super.fields
}


class AnalyzeUncertainty(db: Database) extends LazyLogging {

	val NUM_SAMPLES = 1000
	val NUM_EXAMPLE_TRIALS = 20
	val NUM_FINAL_EXAMPLES = 3
	val rnd = new Random();
	
	def explainRow(oper: Operator, token: RowIdPrimitive): RowExplanation =
	{
		val singleRowQuery = filterByProvenance(oper, token)
		val reasonSets =
			explainSubset(singleRowQuery, Set(), true, false)

		logger.debug(s"EXPLAINED: \n${reasonSets.map(_.toString).mkString("\n")}")

		// Oliver Says: 
		// This try block is a hack: getProvenance only really works on non-aggregate
		// queries, since we can't (easily) expand out aggregates.  For now, we just skip
		// the provenance computation if getProvenance blows up.
		// The right way to implement this is to use MCDB style evaluation (which doesn't 
		// exist yet).
		val probability = 
			try {
				val (tuple, _, provenance) = getProvenance(oper, token)
				val probability = 
					if(CTables.isDeterministic(provenance)){ 1.0 }
					else { 
						sampleExpression[(Int,Int)](provenance, tuple, NUM_SAMPLES, (0,0), 
							(cnt: (Int,Int), present: PrimitiveValue) => 
								present match {
									case NullPrimitive() => (cnt._1, cnt._2)
									case BoolPrimitive(t) => 
										( cnt._1 + (if(t){ 1 } else { 0 }),
										  cnt._2 + 1
										 )
									case _ => throw new RAException("Expecting Boolean from Samples")
								}
						) match { 
							case (_, 0) => -1.0
							case (hits, total) => hits.toDouble / total.toDouble
						}
					}
				logger.debug(s"tuple: $tuple\ncondition: $provenance\nprobability: $probability")
				probability
			} catch {
				case x:RAException => -1.0
			}

		RowExplanation(
			probability,
			getFocusedReasons(reasonSets).toList,
			token
		)
	}

	def explainCell(oper: Operator, token: RowIdPrimitive, column: ID): CellExplanation =
	{
		logger.debug(s"ExplainCell INPUT: $oper")
		val (tuple, allExpressions, _) = getProvenance(oper, token)
		logger.debug(s"ExplainCell Provenance: $allExpressions")
		val expr = allExpressions(column)
		val guessExpr = db.compiler.optimize(Eval.inline(InlineVGTerms(expr, db), tuple))
		val colType = db.typechecker.typeOf(guessExpr)

		val examples = 
			sampleExpression[List[PrimitiveValue]](
				expr, tuple, NUM_EXAMPLE_TRIALS, 
				List[PrimitiveValue](), 
				(_++List(_)) 
			).toSet.take(NUM_FINAL_EXAMPLES)

		colType match {
			case (TInt() | TFloat()) =>
				val (avg, stddev) = getStats(expr, tuple, NUM_SAMPLES)

				NumericCellExplanation(
					avg, 
					stddev, 
					examples.toSet.toList,
					getFocusedReasons(expr, tuple).values.toList,
					token, 
					column
				)

			case _ => 
				GenericCellExplanation(
					examples.toSet.toList,
					getFocusedReasons(expr, tuple).values.toList,
					token,
					column
				)
		}
	}

	def sampleExpression[A](
		expr: Expression, bindings: Map[ID, PrimitiveValue], count: Int, 
		init: A, accum: ((A, PrimitiveValue) => A)
	): A = 
	{
		val sampleExpr = ExpressionDeterminism.compileSample(expr, Var(CTables.SEED_EXP), db.models.get(_))
        (0 until count).
        	map( (i) => 
        		try {
	        		db.interpreter.eval(
	        			sampleExpr, 
		        		bindings ++ Map(CTables.SEED_EXP -> IntPrimitive(rnd.nextInt()))
		        	)
		        } catch {
		        	// Cases like the type inference lens might lead to the expression
		        	// being impossible to evaluate.  Treat these as Nulls
		        	case _:TypeException => NullPrimitive()
		        }
        	).
	        foldLeft(init)(accum)

	}

	def getStats(expr: Expression, tuple: Map[ID,PrimitiveValue], desiredCount: Integer): 
		(PrimitiveValue, PrimitiveValue) =
	{
		val (tot, totSq, realCount) =
			sampleExpression[(PrimitiveValue, PrimitiveValue, Int)](
				expr, tuple, desiredCount,
				(IntPrimitive(0), IntPrimitive(0), 0),
				{ case ((tot, totSq, ct), v) => 
					try {
						(
							Eval.applyArith(Arith.Add, tot, v),
							Eval.applyArith(Arith.Add, totSq,
								Eval.applyArith(Arith.Mult, v, v)),
							ct + 1
						)
	        } catch {
	        	// Cases like the type inference lens might lead to the expression
	        	// being non-numeric.  Simply skip these values.
	        	case _:TypeException => (tot, totSq, ct)
	        }
				}
			)
		if(realCount > 0){
			val avg = Eval.applyArith(Arith.Div, tot, FloatPrimitive(realCount.toDouble))
			val stddev =
				db.interpreter.eval(
					Function("sqrt", 					
						Function("absolute", 
							Arithmetic(Arith.Sub, 
								Arithmetic(Arith.Div, totSq, FloatPrimitive(realCount.toDouble)),
								Arithmetic(Arith.Mult, avg, avg)
							)
						)
					)
				)
			
			(avg, stddev)
		} else {
			(StringPrimitive("n/a"), StringPrimitive("n/a"))
		}
	}

	def getFocusedReasons(reasonSets: Seq[ReasonSet]): Seq[Reason] =
	{
		reasonSets.flatMap { reasonSet => 
			logger.trace(s"Trying to Expand: $reasonSet")
			val subReasons = reasonSet.take(db, 4).toSeq
			if(subReasons.size > 3){
				logger.trace("   -> Too many explanations to fit in one group")
				Seq(new MultiReason(db, reasonSet))
			} else {
				logger.trace(s"   -> Only ${subReasons.size} explanations")
				subReasons
			}
		}
	}

	def getFocusedReasons(expr: Expression, tuple: Map[ID,PrimitiveValue]):
		Map[ID,Reason] =
	{
		logger.trace(s"GETTING REASONS: $expr")
		expr match {
			case v: VGTerm => Map(v.name -> 
				new ModelReason(
					db.models.get(v.name), 
					v.idx, 
					v.args.map { arg => 
						db.interpreter.eval(InlineVGTerms(arg, db),tuple)
					}, 
					v.hints.map { arg =>
						db.interpreter.eval(InlineVGTerms(arg, db),tuple)
					}
				)
			)

			case DataWarning(name, value, message, key, idx) => Map(name -> 
				new DataWarningReason(
					db.models.get(name),
					idx,
					db.interpreter.eval(InlineVGTerms(value, db), tuple),
					db.interpreter.eval(InlineVGTerms(value, db), tuple) match {
						case NullPrimitive() => s"Error reading warning ($name(${key.mkString(",")}))"
						case message => message.asString
					},
					key.map { arg => 
						db.interpreter.eval(InlineVGTerms(arg, db),tuple)
					}
				)
			)

			case Conditional(c, t, e) =>
				(
					if(db.interpreter.evalBool(InlineVGTerms(c, db), tuple)){
						getFocusedReasons(t, tuple)
					} else {
						getFocusedReasons(e, tuple)
					}
				) ++ getFocusedReasons(c, tuple);

			case Arithmetic(op @ (Arith.And | Arith.Or), a, b) =>
				(
					(op, db.interpreter.evalBool(InlineVGTerms(a, db), tuple)) match {
						case (Arith.And, true) => getFocusedReasons(b, tuple)
						case (Arith.Or, false) => getFocusedReasons(b, tuple)
						case _ => Map()
					}
				) ++ getFocusedReasons(a, tuple)

			case _ => 
				expr.children.
					map( getFocusedReasons(_, tuple) ).
					foldLeft(Map[ID,Reason]())(_++_)
		}
	}

	def filterByProvenance(rawOper: Operator, token: RowIdPrimitive): Operator =
	{
		val oper = new PropagateEmptyViews(db.typechecker, db.aggregates)(rawOper)
		logger.debug(s"RESOLVED: \n$oper")
		val (provQuery, rowIdCols) = Provenance.compile(oper)
		val filteredQuery =
			InlineProjections(
				PushdownSelections(
					Provenance.filterForToken(provQuery, token, rowIdCols, db)
				)
			)
		logger.debug(s"FILTERED: \n$filteredQuery")
		return filteredQuery
	}

	def getProvenance(oper: Operator, token: RowIdPrimitive): 
		(Map[ID, PrimitiveValue], Map[ID, Expression], Expression) =
	{
		// Annotate the query to produce a provenance trace
		val (provQuery, rowIdCols) = Provenance.compile(oper)

		// Generate a split token
		val rowIdTokenMap = Provenance.rowIdMap(token, rowIdCols)

		// Flatten the query out into a set of expressions and a row condition
		val (tracedQuery, columnExprs, rowCondition) = Tracer.trace(provQuery, rowIdTokenMap)

		logger.debug(s"TRACE: $tracedQuery")
		logger.debug(s"EXPRS: $columnExprs")
		logger.debug(s"ROW: $rowCondition")

		val inlinedQuery = InlineVGTerms(tracedQuery, db)

		logger.debug(s"INLINE: $inlinedQuery")

		val optQuery = db.compiler.optimize(inlinedQuery)

		val finalSchema = db.typechecker.schemaOf(optQuery)

		//val sqlQuery = db.ra.convert(optQuery)

		//logger.debug(s"SQL: $sqlQuery")

		val results = db.compiler.compileToSparkWithRewrites(optQuery)//sqlQuery)

		val baseData = 
			SparkUtils.extractAllRows(results, finalSchema.map(_._2)).flush

		if(baseData.isEmpty){
			val resultRowString = baseData.map( _.mkString(", ") ).mkString("\n")
			logger.debug(s"Results: $resultRowString")
			throw new InvalidProvenance(""+baseData.size+" rows for token", token)
		}	

		val tuple = finalSchema.map(_._1).zip(baseData.head).toMap

		(tuple, columnExprs, rowCondition)
	}

	def mergeReasons(reasons: Seq[ReasonSet]): Seq[ReasonSet] =
	{
		reasons.
			groupBy { r => (r.model.name, r.idx) }.
			values.
			map { 
				case Seq(singleReason) => 
					logger.debug(s"Not Merging ${singleReason.model};${singleReason.idx}")
					singleReason
				case multipleReasons => {
					logger.debug(s"Merging ${multipleReasons.size} reason sources for ${multipleReasons.head.model};${multipleReasons.head.idx}")
					val (allReasonLookups: Seq[Operator], allReasonArgs: Seq[Seq[Var]], allReasonHints: Seq[Seq[Var]]) =
						multipleReasons.
							flatMap { r => r.argLookup }.
							map { case (query, args, hints) => 
								(
									Project(
										args.zipWithIndex.map { case (expr, i) => ProjectArg(ID("ARG_"+i), expr) }++
										hints.zipWithIndex.map { case (expr, i) => ProjectArg(ID("HINT_"+i), expr) },
										query
									),
									args.zipWithIndex.map  { "ARG_"+_._2  }.map { ID(_) }.map(Var(_)),
									hints.zipWithIndex.map { "HINT_"+_._2 }.map { ID(_) }.map(Var(_))
								)
							}.unzip3
					logger.debug(s" ... ${allReasonLookups.mkString("\n ... ")}")
					// A bit of a simplification... Going to assume for now that we're consistent
					// with how we use VGTerms throughout and that two identical VGTerms should have
					// identical ARG/HINT schemas. 
					// -Oliver
					new ReasonSet(
						multipleReasons.head.model, 
						multipleReasons.head.idx, 
						if(allReasonLookups.isEmpty){ None }
						else { 
							val jointQuery = 
								OperatorUtils.makeUnion(
									allReasonLookups
								)
							Some( (
								jointQuery.distinct,
								allReasonArgs.head,
								allReasonHints.head
							) ) },
						multipleReasons.head.toReason
					)
				}
			}.toSeq
	}

	def explainEverything(oper: Operator): Seq[ReasonSet] = 
	{
		logger.debug(s"Explain Everything: \n$oper")
		val ret = mergeReasons(explainSubset(oper, oper.columnNames.toSet, true, true))
		logger.trace(s"Done Explaining Everything in: \n$oper")
		return ret
	}

	def explainSubset(
		oper: Operator, 
		wantCol: Set[ID], 
		wantRow:Boolean, 
		wantSort:Boolean
	): Seq[ReasonSet] =
		explainSubsetWithoutOptimizing(db.compiler.optimize(oper), wantCol, wantRow, wantSort)

  private def compileCausalityForLens(lensName:ID)(expr: Expression): Seq[(Expression, UncertaintyCausingExpression)] = {
    val lensModels = db.models.associatedModels(lensName)
    ExpressionDeterminism.compileCausality(expr).filter(p => lensModels.contains(p._2.name))
  }

  def explainTableCoarseGrained(schema: ID, table: ID): Seq[ReasonSet] =
  {
  	db.catalog.getDependencies((schema, table))
  		.flatMap { 
  			case CoarseDependency(sourceSchema, source) => 
  				explainEverything(db.catalog.tableOperator(sourceSchema, source))
  		}
  }
		
	def explainSubsetWithoutOptimizing(
		oper: Operator, 
		wantCol: Set[ID], 
		wantRow:Boolean, 
		wantSort:Boolean,
		wantSchema:Boolean = true,
		forLens:Option[ID] = None
	): Seq[ReasonSet] =
	{
	  val compileCausality = forLens match {
	    case None => ExpressionDeterminism.compileCausality _
	    case Some(lensName) =>  compileCausalityForLens(lensName) _
	  }
		logger.trace(s"Explain Subset (${wantCol.mkString(", ")}; $wantRow; $wantSort): \n$oper")
		oper match {
			case Table(name,schema,_,_) => 
				explainTableCoarseGrained(schema, name)
			case HardTable(_,_) => Seq()
			case View(name,query,_) => 
				explainTableCoarseGrained(ViewManager.SCHEMA, name) ++
				explainSubsetWithoutOptimizing(query, wantCol, wantRow, wantSort, wantSchema)

			case AdaptiveView(model, name, query, _) => {
				// Source 1: Recur:  
				logger.debug(s"Explain Adaptive View Source 1: Recursion into $model.$name")
				val sourceReasons = 
					explainSubsetWithoutOptimizing(query, wantCol, wantRow, wantSort, wantSchema)
					
				val (tableReasons, attrReasons) = if(wantSchema){
  				// Model details
  				val (multilens, config) = 
  						db.adaptiveSchemas.get(model) match {
  							case Some(cfg) => cfg
  							case None => throw new RAException(s"Unknown adaptive schema '$model'")
  						}
  
  				// Source 2: There might be uncertainty on the table.  Use SYS_TABLES to dig these annotations up.
  				logger.debug(s"Explain Adaptive View Source 2: $model.$name")
  				val tableReasons = explainSubsetWithoutOptimizing(
  					multilens.tableCatalogFor(db, config).filter( Var(ID("TABLE_NAME")).eq(StringPrimitive(name.id)) ), wantCol, wantRow, wantSort, wantSchema
  				)
  				// alternative: Use SYS_TABLES directly
  				//    db.table("SYS_TABLES").where( Var("SCHEMA").eq(StringPrimitive(model)).and( Var("TABLE").eq(StringPrimitive(name)) ) )
  
  				// Source 3: Check for uncertainty in one of the attributes of interest
  				logger.debug(s"Explain Adaptive View Source 3: $model.$name attributes")
  				val attrReasons = explainEverything(
  					multilens.attrCatalogFor(db, config)
  									 .filter { Var(ID("TABLE_NAME")).eq(StringPrimitive(name.id))
  									 						.and { Var(ID("ATTR_NAME")).in( 
  									 											wantCol.toSeq
  									 														 .map { _.id }
  									 														 .map { StringPrimitive(_) } 
  									 									 ) }
  									 					}
  				)
  				(tableReasons, attrReasons)
				} else (Seq(), Seq())
				logger.debug(s"Explain Adaptive View Done: $model.$name")
				// alternative: Use SYS_ATTRS directly
				//    db.table("SYS_TABLES").where( Var("SCHEMA").eq(StringPrimitive(model)).and( Var("TABLE").eq(StringPrimitive(name)) ).and( Var("ATTR").in(wantCol.map(StringPrimitive(_))) ) )
				sourceReasons ++ tableReasons ++ attrReasons ++
					explainTableCoarseGrained(model, name)
			}


			case Project(args, child) => {
				val relevantArgs =
					args.filter { col => wantCol(col.name) }

				val argReasons = 
					relevantArgs.
						flatMap {
						  col => {
						    val compiledCausalityExpr = compileCausality(col.expression)
						    compiledCausalityExpr.map {
						      case (condition, uncertain) => (uncertain.toString, (condition, uncertain))
						    }.toMap.toSeq.map(_._2)
						  }
						}.map { case (condition, uncertain) => 
							ReasonSet.make(uncertain, db, Select(condition, child))
						}
				val argDependencies =
					relevantArgs.
						flatMap {
							col => ExpressionUtils.getColumns(col.expression)
						}.toSet

				argReasons ++ explainSubsetWithoutOptimizing(child, argDependencies, wantRow, wantSort, wantSchema)
			}

			// Keep views unless we can push selections down into them
			case Select(cond, View(_,query,_)) => 
				explainSubsetWithoutOptimizing(Select(cond, query), wantCol, wantRow, wantSort, wantSchema)

			case Select(cond, child) => {
				val (condReasons:Seq[ReasonSet], condDependencies:Set[ID]) =
					if(wantRow){
						(
							compileCausality(cond).map {
						      case (condition, uncertain) => (uncertain.toString, (condition, uncertain))
						    }.toMap.toSeq.map(_._2).
								map { case (condition, uncertain) => 
										ReasonSet.make(uncertain, db, Select(condition, child))
									},
							ExpressionUtils.getColumns(cond)
						)
					} else { ( Seq(), Set() ) }

				condReasons ++ explainSubsetWithoutOptimizing(child, wantCol ++ condDependencies, wantRow, wantSort, wantSchema)
			}

			case Aggregate(gbs, aggs, child) => {
				val aggVGTerms = 
					aggs.flatMap { agg => agg.args.flatMap( compileCausality(_).map {
						      case (condition, uncertain) => (uncertain.toString, (condition, uncertain))
						    }.toMap.toSeq.map(_._2) ) }
				val aggReasons =
					aggVGTerms.map { case (condition, uncertain) => 
						ReasonSet.make(uncertain, db, Select(condition, child))
					}
				val gbDependencies =
					gbs.map( _.name ).filter { gb => wantRow || wantCol(gb) }.toSet
				val aggDependencies =
				  aggs.
				  	filter { agg => wantCol(agg.alias) }.
						flatMap { agg => 
							agg.args.flatMap( ExpressionUtils.getColumns(_) )
						}.toSet

				aggReasons ++ explainSubsetWithoutOptimizing(child, 
					// We need all of the input dependencies
					gbDependencies ++ aggDependencies, 
					// If we want the output row presence, input row reasons are relevant
					// If there are any agg dependencies, input row reasons are also relevant
					wantRow || !aggDependencies.isEmpty,
					// Sort is never relevant for aggregates
					false, wantSchema
				)
			}

			case Join(lhs, rhs) => {
				explainSubsetWithoutOptimizing(lhs, lhs.columnNames.filter(wantCol(_)).toSet, wantRow, wantSort, wantSchema) ++ 
				explainSubsetWithoutOptimizing(rhs, rhs.columnNames.filter(wantCol(_)).toSet, wantRow, wantSort, wantSchema)
			}

			case LeftOuterJoin(left, right, cond) => 
        throw new RAException("Don't know how to explain a left-outer-join")

      case Limit(_, _, child) => 
      	explainSubsetWithoutOptimizing(child, wantCol, wantRow, wantSort || wantRow, wantSchema)

     	case Sort(args, child) => {
				val (argReasons, argDependencies) =
					if(wantSort){
						(
							args.flatMap { arg => 
								compileCausality(arg.expression).map {
						      case (condition, uncertain) => (uncertain.toString, (condition, uncertain))
						    }.toMap.toSeq.map(_._2)
							}.map { case (condition, uncertain) => 
								ReasonSet.make(uncertain, db, Select(condition, child))
							},
							args.flatMap { arg => ExpressionUtils.getColumns(arg.expression) }
						)
					} else { (Seq(),Set()) }

				argReasons ++ explainSubsetWithoutOptimizing(child,argDependencies.toSet ++ wantCol,wantRow,wantSort, wantSchema)
			}

			case Union(lhs, rhs) => {
				explainSubsetWithoutOptimizing(lhs,wantCol,wantRow,wantSort, wantSchema) ++ 
				explainSubsetWithoutOptimizing(rhs,wantCol,wantRow,wantSort, wantSchema)
			}

			case DrawSamples(mode, source, seed, caveat) => {
				(
					explainSubsetWithoutOptimizing(source,wantCol,wantRow,wantSort,wantSchema) ++
					caveat.map { case (modelName, message) =>
						val model = db.models.get(modelName)
						new ReasonSet(
							model,
							0,
							None,
							(k, v) => new DataWarningReason(model, 0, NullPrimitive(), message, Seq())
						)
					}
				)
			}
		}
	}
		
	def explainAdaptiveSchema(oper: Operator, 
		wantCol: Set[ID], 
		wantTable:Boolean
	): Seq[ReasonSet] =
	{
		logger.trace(s"Explain AdaptiveSchema (${wantCol.mkString(", ")}; $wantTable): \n$oper")
		val recur = explainAdaptiveSchema(_:Operator, wantCol, wantTable)
		oper match {
			case AdaptiveView(model, name, query, _) => {
				val sourceReasons = 
					explainAdaptiveSchema(query, wantCol, wantTable)
					
			  val (multilens, config) = 
						db.adaptiveSchemas.get(model) match {
							case Some(cfg) => cfg
							case None => throw new RAException(s"Unknown adaptive schema '$model'")
						}

				// Source 1: There might be uncertainty on the table.  Use SYS_TABLES to dig these annotations up.
				logger.debug(s"Explain Adaptive View Source 1: $model.$name")
				val tableReasons = if(wantTable) explainEverything(
					multilens.tableCatalogFor(db, config).filter( ID("TABLE_NAME").eq(name) )
				) else Seq()
				
				// Source 2: Check for uncertainty in one of the attributes of interest
				logger.debug(s"Explain Adaptive View Source 2: $model.$name attributes")
				val attrReasons = explainEverything(
					multilens.attrCatalogFor(db, config)
									 .filter { ID("TABLE_NAME").eq(name)
									 						.and { ID("ATTR_NAME").in( 
									 										wantCol.toSeq
									 													 .map { _.id }
									 													 .map { StringPrimitive(_) } 
									 								)}
									 					}
				)
				logger.debug(s"Explain Adaptive View Done: $model.$name")
				sourceReasons ++ tableReasons ++ attrReasons
			}
			case _ => oper.children.flatMap(recur(_))
	  }
	}
}
