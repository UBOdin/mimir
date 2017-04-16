package mimir.exec

import mimir.Database
import mimir.optimizer._
import mimir.algebra._
import mimir.ctables._
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * TupleBundles ( http://dl.acm.org/citation.cfm?id=1376686 ) are a tactic for
 * computing over probabilistic data.  Loosely put, the approach is to compile
 * the query to evaluate simultaneously in N possible worlds.  The results can
 * then be aggregated to produce an assortment of statistics, etc...
 *
 * This class actually wraps three different compilation strategies inspired
 * by tuple bundles, each handling parallelization in a slightly different way
 * 
 * * **Long**:  Not technically "TupleBundles".  This approach simply unions 
 * *            together a set of results, one per possible world sampled.
 * * **Flat**:  Creates a wide result, splitting each non-deterministic column
 *              into a set of columns, one per sample.
 * * **Array**: Like flat, but uses native array types to avoid overpopulating
 *              the result schema.
 *
 * At present, only 'Flat' is fully implemented, although a 'Long'-like approach
 * can be achieved by using convertFlatToLong.
 */

class TupleBundler(db: Database, sampleSeeds: Seq[Int] = (0 until 10))
  extends LazyLogging
{
  def colNameInSample(col: String, i: Int): String =
    s"MIMIR_SAMPLE_${i}_$col"

  def sampleCols(col: String): Seq[String] =
  {
    (0 until sampleSeeds.size).map { i => colNameInSample(col, i) }
  }

  def fullBitVector =
    (0 until sampleSeeds.size).map { 1 << _ }.fold(0)( _ | _ )

  def apply(query: Operator): Operator =
  {
    val (compiled, nonDeterministicColumns) = compileFlat(query)    
    Project(
      query.schema.map(_._1).map { col => 
        ProjectArg(col, 
          if(nonDeterministicColumns(col)){
            Function("JSON_ARRAY", sampleCols(col).map( Var(_) ))
          } else {
            Var(col) 
          }
        )} ++ Seq(ProjectArg("MIMIR_WORLD_BITS", Var("MIMIR_WORLD_BITS"))), 
      compiled
    )
  }

  def doesExpressionNeedSplit(expression: Expression, nonDeterministicInputs: Set[String]): Boolean =
  {
    val allInputs = ExpressionUtils.getColumns(expression)
    val expressionHasANonDeterministicInput =
      allInputs.exists { nonDeterministicInputs(_) }
    val expressionIsNonDeterministic =
      !CTables.isDeterministic(expression)

    return expressionHasANonDeterministicInput || expressionIsNonDeterministic
  }

  def splitExpressionsByWorlds(expressions: Seq[Expression], nonDeterministicInputs: Set[String]): Seq[Seq[Expression]] =
  {
    val outputColumns =
      sampleSeeds.zipWithIndex.map { case (seed, i) => 
        val inputInstancesInThisSample = 
          nonDeterministicInputs.
            map { x => (x -> Var(colNameInSample(x, i)) ) }.
            toMap
        expressions.map { expression => 
          CTAnalyzer.compileSample(
            Eval.inline(expression, inputInstancesInThisSample),
            IntPrimitive(seed)
          )
        }
      }

    outputColumns
  }

  def splitExpressionByWorlds(expression: Expression, nonDeterministicInputs: Set[String]): Seq[Expression] =
  {
    splitExpressionsByWorlds(Seq(expression), nonDeterministicInputs).map(_(0))
  }

  def convertFlatToLong(compiledQuery: Operator, baseSchema: Seq[String], nonDeterministicInput: Set[String]): Operator =
  {
    val sampleShards =
      (0 until sampleSeeds.size).map { i =>
        val mergedSamples =
          baseSchema.map { col =>
            ProjectArg(col, 
              if(nonDeterministicInput(col)){ Var(colNameInSample(col, i)) }
              else { Var(col) }
            )
          } ++ Seq(
            ProjectArg("MIMIR_WORLD_BITS", IntPrimitive(1 << i))
          )

        val filterWorldPredicate =
          Comparison(Cmp.Eq,
            Arithmetic(Arith.BitAnd, 
              Var("MIMIR_WORLD_BITS"),
              IntPrimitive(1 << i)
            ),
            IntPrimitive(1 << i)
          )
          
        InlineProjections(
          ProjectRedundantColumns(
            Project(
              mergedSamples, 
              PushdownSelections(
                Select(filterWorldPredicate, compiledQuery)
              )
            )
          )
        )
      }

    logger.trace(s"Converting FlatToLong: \n$compiledQuery\n ---> TO:\n${sampleShards(0)}")

    OperatorUtils.makeUnion(sampleShards)
  }

  def compileFlat(query: Operator): (Operator, Set[String]) =
  {
    // Check for a shortcut opportunity... if the expression is deterministic, we're done!
    if(CTables.isDeterministic(query)){
      return (
        OperatorUtils.projectInColumn(
          "MIMIR_WORLD_BITS", IntPrimitive(fullBitVector), 
          query
        ),
        Set[String]()
      )
    }
    query match {
      case (Table(_,_,_) | EmptyTable(_)) => 
        (
          OperatorUtils.projectInColumn(
            "MIMIR_WORLD_BITS", IntPrimitive(fullBitVector), 
            query
          ),
          Set[String]()
        )

      case Project(columns, oldChild) => {
        val (newChild, nonDeterministicInput) = compileFlat(oldChild)

        val (
          newColumns,
          nonDeterministicOutputs
        ):(Seq[Seq[ProjectArg]], Seq[Set[String]]) = columns.map { col => 
            if(doesExpressionNeedSplit(col.expression, nonDeterministicInput)){
              (
                splitExpressionByWorlds(col.expression, nonDeterministicInput).
                  zipWithIndex
                  map { case (expr, i) => ProjectArg(colNameInSample(col.name, i), expr) },
                Set(col.name)
              )
            } else {
              (Seq(col), Set[String]())
            }
          }.unzip

        val replacementProjection =
          Project(
            newColumns.flatten ++ Seq(ProjectArg("MIMIR_WORLD_BITS", Var("MIMIR_WORLD_BITS"))),
            newChild
          )

        (replacementProjection, nonDeterministicOutputs.flatten.toSet)
      }

      case Select(condition, oldChild) => {
        val (newChild, nonDeterministicInput) = compileFlat(oldChild)

        if(doesExpressionNeedSplit(condition, nonDeterministicInput)){
          val replacements = splitExpressionByWorlds(condition, nonDeterministicInput)

          val updatedWorldBits =
            Arithmetic(Arith.BitAnd,
              Var("MIMIR_WORLD_BITS"),
              replacements.zipWithIndex.map { case (expr, i) =>
                Conditional(expr, IntPrimitive(1 << i), IntPrimitive(0))
              }.fold(IntPrimitive(0))(Arithmetic(Arith.BitOr, _, _))
            )

          logger.debug(s"Updated World Bits: \n${updatedWorldBits}")
          val newChildWithUpdatedWorldBits =
            OperatorUtils.replaceColumn(
              "MIMIR_WORLD_BITS",
              updatedWorldBits,
              newChild
            )
          (
            Select(
              Comparison(Cmp.Neq, Var("MIMIR_WORLD_BITS"), IntPrimitive(0)),
              newChildWithUpdatedWorldBits
            ),
            nonDeterministicInput
          )
        } else {
          ( Select(condition, newChild), nonDeterministicInput )
        }
      }

      case Join(lhsOldChild, rhsOldChild) => {
        val (lhsNewChild, lhsNonDeterministicInput) = compileFlat(lhsOldChild)
        val (rhsNewChild, rhsNonDeterministicInput) = compileFlat(rhsOldChild)

        // To safely join the two together, we need to rename the world-bit columns
        val rewrittenJoin =
          OperatorUtils.joinMergingColumns(
            Seq( ("MIMIR_WORLD_BITS",
                    (lhs:Expression, rhs:Expression) => Arithmetic(Arith.BitAnd, lhs, rhs))
            ),
            lhsNewChild, rhsNewChild
          )

        // Finally, add a selection to filter out values that can be filtered out in all worlds.
        val completedJoin =
          Select(
            Comparison(Cmp.Neq, Var("MIMIR_WORLD_BITS"), IntPrimitive(0)),
            rewrittenJoin
          )

        (completedJoin, lhsNonDeterministicInput ++ rhsNonDeterministicInput)
      }

      case Union(lhsOldChild, rhsOldChild) => {
        val (lhsNewChild, lhsNonDeterministicInput) = compileFlat(lhsOldChild)
        val (rhsNewChild, rhsNonDeterministicInput) = compileFlat(rhsOldChild)
        val schema = query.schema.map(_._1)

        val alignNonDeterminism = (
          query: Operator, 
          nonDeterministicInput: Set[String], 
          nonDeterministicOutput: Set[String]
        ) => {
          Project(
            schema.flatMap { col => 
              if(nonDeterministicOutput(col)){
                if(nonDeterministicInput(col)){
                  sampleCols(col).map { sampleCol => ProjectArg(sampleCol, Var(sampleCol)) }
                } else {
                  sampleCols(col).map { sampleCol => ProjectArg(sampleCol, Var(col)) }
                }
              } else {
                if(nonDeterministicInput(col)){
                  throw new RAException("ERROR: Non-deterministic inputs must produce non-deterministic outputs")
                } else {
                  Seq(ProjectArg(col, Var(col)))
                }
              }
            },
            query
          )
        }

        val nonDeterministicOutput = 
          lhsNonDeterministicInput ++ rhsNonDeterministicInput
        (
          Union(
            alignNonDeterminism(lhsNewChild, lhsNonDeterministicInput, nonDeterministicOutput),
            alignNonDeterminism(rhsNewChild, rhsNonDeterministicInput, nonDeterministicOutput)
          ), 
          nonDeterministicOutput
        )
      }

      case Aggregate(gbColumns, aggColumns, oldChild) => {
        val (newChild, nonDeterministicInput) = compileFlat(oldChild)

        // We divide the aggregate into two cases, an easy one and a hard one.
        //
        // The easy case is when all of the group-by columns are deterministic.
        // If that's all, we know that the groups themselves are deterministic, and
        // can guarantee that we fold each tuple into the right group.  We still
        // need to split one or more aggregate functions into possible-worlds, but
        // the core of the expression stays fixed.
        //
        // The hard case is when even one of the group-by columns is 
        // non-deterministic.  When that happens, classical aggregation no longer
        // really works in the same way, since a single tuple may contribute to 
        // potentially multiple groups.  What we do in that case is resort to 'Long'
        // rewriting, creating one tuple for every possible world.  These tuples then
        // get aggregated.
        // 

        val oneOfTheGroupByColumnsIsNonDeterministic =
          gbColumns.map(_.name).exists(nonDeterministicInput(_))

        if(oneOfTheGroupByColumnsIsNonDeterministic){
          logger.trace(s"Processing non-deterministic aggregate: \n$query")

          // This is the hard case: One of the group-by columns is non-deterministic
          // We need to resort to evaluating the query with the 'Long' evaluation strategy.
          // If we created our own evaluation engine, we could get around this limitation, but
          // as long as we need to run on a normal backend, this is required.
          val shardedChild = convertFlatToLong(newChild, oldChild.schema.map(_._1), nonDeterministicInput)

          // Split the aggregate columns.  Because a group-by attribute is uncertain, all
          // sources of uncertainty can be, potentially, non-deterministic.
          // As a result, we convert expressions to aggregates over case statements:  
          // i.e., SUM(A) AS A becomes
          // SUM(CASE WHEN inputIsInWorld(1) THEN A ELSE NULL END) AS A_1,
          // SUM(CASE WHEN inputIsInWorld(2) THEN A ELSE NULL END) AS A_2,
          // ...
          val (splitAggregates, nonDeterministicOutputs) =
            aggColumns.map { case AggFunction(name, distinct, args, alias) =>
              val splitAggregates =
                (0 until sampleSeeds.size).map { i =>
                  AggFunction(name, distinct, 
                    args.map { arg => 
                      Conditional(
                        Comparison(Cmp.Eq,
                          Arithmetic(Arith.BitAnd, 
                            Var("MIMIR_WORLD_BITS"),
                            IntPrimitive(1 << i)
                          ),
                          IntPrimitive(1 << i)
                        ),
                        arg,
                        NullPrimitive()
                      )
                    },
                    colNameInSample(alias, i)
                  )
                }
              (splitAggregates, Set(alias))
            }.unzip

          // We also need to figure out which worlds each group will be present in.
          // We take an OR of all of the worlds that lead to the aggregate being present.
          val worldBitsAgg = 
            AggFunction("GROUP_BITWISE_OR", false, Seq(Var("MIMIR_WORLD_BITS")), "MIMIR_WORLD_BITS")

          (
            Aggregate(gbColumns, splitAggregates.flatten ++ Seq(worldBitsAgg), shardedChild),
            nonDeterministicOutputs.flatten.toSet
          )
          
        } else {

          logger.trace(s"Processing deterministic aggregate: \n$query")

          // This is the easy case: All of the group-by columns are non-deterministic 
          // and we can safely use classical aggregation to compute this expression.

          // As before we may need to split aggregate columns, but here we can first
          // check to see if the aggregate expression depends on non-deterministic 
          // values.  If it does not, then we can avoid splitting it.
          val (splitAggregates, nonDeterministicOutputs) =
            aggColumns.map { case AggFunction(name, distinct, args, alias) =>
              if(args.exists(doesExpressionNeedSplit(_, nonDeterministicInput))){
                val splitAggregates =
                  splitExpressionsByWorlds(args, nonDeterministicInput).
                    zipWithIndex.
                    map { case (newArgs, i) => AggFunction(name, distinct, newArgs, colNameInSample(alias, i)) }
                (splitAggregates, Set(alias))
              } else {
                (Seq(AggFunction(name, distinct, args, alias)), Set[String]())
              }
            }.unzip

          // Same deal as before: figure out which worlds the group will be present in.

          val worldBitsAgg = 
            AggFunction("GROUP_BITWISE_OR", false, Seq(Var("MIMIR_WORLD_BITS")), "MIMIR_WORLD_BITS")

          (
            Aggregate(gbColumns, splitAggregates.flatten ++ Seq(worldBitsAgg), newChild),
            nonDeterministicOutputs.flatten.toSet
          )

        }


      }

      // We don't handle materialized tuple bundles (at the moment)
      // so give up and drop the view.
      case View(_, query, _) =>  compileFlat(query)

      case ( Sort(_,_) | Limit(_,_,_) | LeftOuterJoin(_,_,_) ) =>
        throw new RAException("Tuple-Bundler presently doesn't support LeftOuterJoin, Sort, or Limit (probably need to resort to 'Long' evaluation)")

    }
  }
}

object WorldBits
  extends LazyLogging
{
  def isInWorld(bv: Long, worldId: Int): Boolean =
    ((bv & (1 << worldId)) > 0)

  def worlds(bv: Long, numSamples: Int): Set[Int] =
  {
    (0 until numSamples).
      filter( isInWorld(bv, _) ).
      toSet
  }

  def confidence(bv: Long, numSamples: Int): Double =
  {
    val hits = 
      (0 until numSamples).
        count( isInWorld(bv, _) )

    logger.debug(s"Testing: $bv <- $hits bits set")
    hits.toDouble / numSamples.toDouble
  }
}