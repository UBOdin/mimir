package mimir.ctables;
 
import java.util.Random;
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir._
import mimir.algebra._
import mimir.data.{ SchemaProvider, SystemCatalog }
import mimir.exec._
import mimir.exec.mode._
import mimir.lenses.LensManager
import mimir.models._
import mimir.optimizer.operator._
import mimir.provenance._
import mimir.util._
import mimir.views.ViewManager

case class InvalidProvenance(msg: String, token: RowIdPrimitive) 
	extends Exception("Invalid Provenance Token ["+msg+"]: "+token);


class AnalyzeUncertainty(db: Database) extends LazyLogging {

	// def getFocusedReasons(reasonSets: Seq[ReasonSet]): Seq[Reason] =
	// {
	// 	reasonSets.flatMap { reasonSet => 
	// 		logger.trace(s"Trying to Expand: $reasonSet")
	// 		val subReasons = reasonSet.take(db, 4).toSeq
	// 		if(subReasons.size > 3){
	// 			logger.trace("   -> Too many explanations to fit in one group")
	// 			Seq(new MultiReason(db, reasonSet))
	// 		} else {
	// 			logger.trace(s"   -> Only ${subReasons.size} explanations")
	// 			subReasons
	// 		}
	// 	}
	// }

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
			groupBy { r => r.lens }.
			values.
			map { 
				case Seq(singleReason) => 
					logger.debug(s"Not Merging ${singleReason.lens}")
					singleReason
				case multipleReasons => {
					logger.debug(s"Merging ${multipleReasons.size} reason sources for ${multipleReasons.head.lens}")
					val (allMultiReasonLookups: Seq[Operator], allMultiReasonCounts: Seq[Int]) =
						multipleReasons
							.map { _.argLookup }
							.collect { 
								case MultipleArgLookup(query, args, message) => 
									(
										Project(
											args.zipWithIndex.map { case (expr, i) => ProjectArg(ID("ARG_"+i), expr) }
											  :+ ProjectArg(ID("MESSAGE"), message),
											query
										),
										args.length
									)
							}.unzip
					val allSingleReasonLookups =
						multipleReasons
							.map { _.argLookup }
							.collect { case SingleArgLookup(message) => message }
							.flatten
							.toSet.toSeq
					logger.debug(s" ... ${allSingleReasonLookups.mkString("\n ... ")}")
					logger.debug(s" ... ${allMultiReasonLookups.mkString("\n ... ")}")
					// A bit of a simplification... Going to assume for now that we're consistent
					// with how we use VGTerms throughout and that two identical VGTerms should have
					// identical ARG schemas. 
					// -Oliver
					if(!allMultiReasonLookups.isEmpty){
						val singleReasonLookupsQuery: Option[Operator] = 
							if(allSingleReasonLookups.isEmpty){ None }
							else { 
								Some(HardTable(
									Seq( ID("MESSAGE") -> TString() ),
									allSingleReasonLookups.map { StringPrimitive(_) }.map { Seq(_) }
								))
							}
						val jointQuery = 
							OperatorUtils.makeUnion(
								allMultiReasonLookups 
									++ singleReasonLookupsQuery
							)
						val argSize = allMultiReasonCounts.head

						if(!allMultiReasonCounts.forall { _ == argSize }){
							throw new RuntimeException(
								s"Error while merging ReasonSets for ${multipleReasons.head.lens}: Mismatched Arg Counts: $allMultiReasonCounts"
							)
						}
						new ReasonSet(
							multipleReasons.head.lens,
							MultipleArgLookup(
								jointQuery, 
								(0 until argSize).map { i => Var(ID("ARG_"+i)) },
								Var(ID("MESSAGE"))
							)
						)
					} else { // only SingleReasonArgs
						new ReasonSet(
							multipleReasons.head.lens,
							SingleArgLookup(allSingleReasonLookups)
						)
					}
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
		wantSort:Boolean,
		wantSchema:Boolean = true
	): Seq[ReasonSet] =
		explainSubsetWithoutOptimizing(
			oper = db.compiler.optimize(oper), 
			wantCol = wantCol, 
			wantRow = wantRow, 
			wantSort = wantSort, 
			wantSchema = wantSchema
		)

  private def compileCausalityForLens(lensName:ID)(expr: Expression): Seq[(Expression, UncertaintyCausingExpression)] = {
    ExpressionDeterminism.compileCausality(expr)
    									   .filter { case (condition, caveat) => caveat.lens.equals(lensName) }
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
		forLens:Option[ID] = None,
		adaptiveSchemaOnlyCols: Set[ID] = Set()
	): Seq[ReasonSet] =
	{
	  val compileCausality = forLens match {
	    case None => ExpressionDeterminism.compileCausality _
	    case Some(lensName) =>  compileCausalityForLens(lensName) _
	  }
	  def recur(
	  	rcrOper: Operator,
	  	wantCol: Set[ID] = wantCol,
	  	wantRow: Boolean = wantRow,
	  	wantSort: Boolean = wantSort,
	  	wantSchema: Boolean = wantSchema,
	  	forLens: Option[ID] = forLens,
	  	adaptiveSchemaOnlyCols: Set[ID] = adaptiveSchemaOnlyCols
	  ) = explainSubsetWithoutOptimizing(
  		rcrOper, 
	  	wantCol = wantCol,
	  	wantRow = wantRow,
	  	wantSort = wantSort,
	  	wantSchema = wantSchema,
	  	forLens = forLens,
	  	adaptiveSchemaOnlyCols = adaptiveSchemaOnlyCols
  	)

		logger.trace(s"Explain Subset (${wantCol.mkString(", ")}; $wantRow; $wantSort): \n$oper")
		oper match {
			case Table(name,schema,_,_) => 
				explainTableCoarseGrained(schema, name)
			case HardTable(_,_) => Seq()
			case View(name,query,_) => 
				explainTableCoarseGrained(ViewManager.SCHEMA, name) ++ recur(query)

			case LensView(schema, table, query, _) => {
				// Source 1: Recur:  
				val trueSchema = schema.getOrElse{LensManager.SCHEMA}
				val tableNameString = s"$trueSchema.$table"
				logger.debug(s"Explain Adaptive View Source 1: Recursion into $tableNameString")
				val sourceReasons = recur(query)
					
				val (tableReasons, attrReasons) = if(wantSchema){
  				// Model details
  				val schemaProvider: SchemaProvider = 
  					schema match {
  						case None => db.lenses
  						case Some(lens) => db.lenses.schemaProviderFor(lens).get
  					}
  
  				// Source 2: There might be uncertainty on the table.  Use SYS_TABLES to dig these annotations up.
  				logger.debug(s"Explain Adaptive View Source 2: $tableNameString itself")
  				val tableReasons = explainSubsetWithoutOptimizing(
  					schemaProvider.listTablesQuery
  												.filter { Var(ID("TABLE_NAME")).eq(StringPrimitive(table.id)) }, 
  					wantCol = SystemCatalog.tableCatalogSchema.map { _._1 }.toSet,
  					wantRow = true, 
  					wantSort = false, 
  					wantSchema = false,
  					adaptiveSchemaOnlyCols = Set()
  				)
  				// alternative: Use SYS_TABLES directly
  				//    db.table("SYS_TABLES").where( Var("SCHEMA").eq(StringPrimitive(model)).and( Var("TABLE").eq(StringPrimitive(name)) ) )
  
  				// Source 3: Check for uncertainty in one of the attributes of interest
  				logger.debug(s"Explain Adaptive View Source 3: $tableNameString's attributes")
  				val attrReasons = recur(
  					schemaProvider.listAttributesQuery
                          .filter { Var(ID("TABLE_NAME")).eq(StringPrimitive(table.id))
                                      .and { Var(ID("ATTR_NAME")).in( 
                                                (wantCol++adaptiveSchemaOnlyCols)
                                                       .toSeq
                                                       .map { _.id }
                                                       .map { StringPrimitive(_) } 
                                             ) }
                                    },
  					wantCol = SystemCatalog.attrCatalogSchema.map { _._1 }.toSet, 
  					wantRow = true, 
  					wantSort = true, 
  					wantSchema = false,
  					adaptiveSchemaOnlyCols = Set()
  				)
  				(tableReasons, attrReasons)
				} else (Seq(), Seq())
				logger.debug(s"Explain Adaptive View Done: $tableNameString")
				// alternative: Use SYS_ATTRS directly
				//    db.table("SYS_TABLES").where( Var("SCHEMA").eq(StringPrimitive(model)).and( Var("TABLE").eq(StringPrimitive(name)) ).and( Var("ATTR").in(wantCol.map(StringPrimitive(_))) ) )
				sourceReasons ++ tableReasons ++ attrReasons ++
					explainTableCoarseGrained(trueSchema, table)
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

				argReasons ++ recur(child, wantCol = argDependencies)
			}

			// Keep views unless we can push selections down into them
			case Select(cond, View(view,query,_)) => 
				recur(Select(cond, query)) ++ explainTableCoarseGrained(ViewManager.SCHEMA, view)

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

				condReasons ++ recur(child, wantCol = wantCol ++ condDependencies)
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
				val allDependencies = 
					gbs.map { gb => gb.name -> Set(gb.name) }.toMap ++
					aggs.map { agg => agg.alias -> agg.args.flatMap( ExpressionUtils.getColumns(_) ) }.toSet
				val gbDependencies =
					gbs.map( _.name ).filter { gb => wantRow || wantCol(gb) }.toSet
				val aggDependencies =
					wantCol.flatMap { allDependencies(_) }
				         .toSet

				aggReasons ++ recur(child, 
					// We need all of the input dependencies
					wantCol = gbDependencies ++ aggDependencies, 
					// If we want the output row presence, input row reasons are relevant
					// If there are any agg dependencies, input row reasons are also relevant
					wantRow = wantRow || !aggDependencies.isEmpty,
					// Sort is never relevant for aggregates
					wantSort = false,
					adaptiveSchemaOnlyCols = adaptiveSchemaOnlyCols.flatMap { allDependencies(_) }.toSet
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
		}
	}
		
}
