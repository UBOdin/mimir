package mimir.exec

import mimir.Database
import mimir.optimizer._
import mimir.algebra._
import mimir.ctables._
import com.typesafe.scalalogging.slf4j.LazyLogging

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

  def splitExpressions(expressions: Seq[Expression], nonDeterministicInputs: Set[String]): Seq[Seq[Expression]] =
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

  def splitExpression(expression: Expression, nonDeterministicInputs: Set[String]): Seq[Expression] =
  {
    splitExpressions(Seq(expression), nonDeterministicInputs).map(_(0))
  }

  def compileFlat(query: Operator): (Operator, Set[String]) =
  {
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
                splitExpression(col.expression, nonDeterministicInput).
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
          val replacements = splitExpression(condition, nonDeterministicInput)

          val updatedWorldBits =
            Arithmetic(Arith.BitAnd,
              Var("MIMIR_WORLD_BITS"),
              replacements.zipWithIndex.map { case (expr, i) =>
                Conditional(expr, IntPrimitive(1 << i), IntPrimitive(0))
              }.fold(IntPrimitive(0))(Arithmetic(Arith.BitOr, _, _))
            )
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
          Join(
            OperatorUtils.renameColumn(
              "MIMIR_WORLD_BITS", "MIMIR_WORLD_BITS_LEFT", 
              lhsNewChild
            ),
            OperatorUtils.renameColumn(
              "MIMIR_WORLD_BITS", "MIMIR_WORLD_BITS_RIGHT", 
              rhsNewChild
            )
          )

        // Next we need to merge the two world-bit columns
        val joinWithOneWorldBitsColumn =
          OperatorUtils.projectAwayColumns(
            Set("MIMIR_WORLD_BITS_LEFT", "MIMIR_WORLD_BITS_RIGHT"),
            OperatorUtils.projectInColumn(
              "MIMIR_WORLD_BITS", 
              Arithmetic(Arith.BitAnd, Var("MIMIR_WORLD_BITS_LEFT"), Var("MIMIR_WORLD_BITS_RIGHT")),
              rewrittenJoin
            )
          )

        // Finally, add a selection to filter out values that can be filtered out in all worlds.
        val completedJoin =
          Select(
            Comparison(Cmp.Neq, Var("MIMIR_WORLD_BITS"), IntPrimitive(0)),
            joinWithOneWorldBitsColumn
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

        val oneOfTheGroupByColumnsIsNonDeterministic =
          gbColumns.map(_.name).exists(nonDeterministicInput(_))

        if(oneOfTheGroupByColumnsIsNonDeterministic){

          val sampleShards =
            (0 until sampleSeeds.size).map { i =>
              val mergedSamples =
                oldChild.schema.map(_._1).map { col =>
                  ProjectArg(col, 
                    if(nonDeterministicInput(col)){ Var(colNameInSample(col, i)) }
                    else { Var(col) }
                  )
                } ++ Seq(
                  ProjectArg("MIMIR_SAMPLE_ID", IntPrimitive(i))
                )
              
              Project(
                mergedSamples, 
                Select(
                  Comparison(Cmp.Neq,
                    Var("MIMIR_WORLD_BITS"),
                    IntPrimitive(1 << i)
                  ),
                  newChild
                )
              )
            }

          val shardedChild = OperatorUtils.makeUnion(sampleShards)

          val (splitAggregates, nonDeterministicOutputs) =
            aggColumns.map { case AggFunction(name, distinct, args, alias) =>
              if(args.exists(doesExpressionNeedSplit(_, nonDeterministicInput))){
                val splitAggregates =
                  (0 until sampleSeeds.size).map { i =>
                    AggFunction(name, distinct, 
                      args.map { arg => 
                        Conditional(
                          Comparison(Cmp.Eq, Var("MIMIR_SAMPLE_ID"), IntPrimitive(i)),
                          arg,
                          NullPrimitive()
                        )
                      },
                      colNameInSample(alias, i)
                    )
                  }
                (splitAggregates, Set(alias))
              } else {
                (Seq(AggFunction(name, distinct, args, alias)), Set[String]())
              }
            }.unzip

          val worldBitsAgg = 
            AggFunction("GROUP_BITWISE_OR", false, Seq(
              Arithmetic(Arith.ShiftLeft, IntPrimitive(1), Var("MIMIR_SAMPLE_ID"))
            ), "MIMIR_WORLD_BITS")

          (
            Aggregate(gbColumns, splitAggregates.flatten ++ Seq(worldBitsAgg), newChild),
            nonDeterministicOutputs.flatten.toSet
          )
          
        } else {

          val (splitAggregates, nonDeterministicOutputs) =
            aggColumns.map { case AggFunction(name, distinct, args, alias) =>
              if(args.exists(doesExpressionNeedSplit(_, nonDeterministicInput))){
                val splitAggregates =
                  splitExpressions(args, nonDeterministicInput).
                    zipWithIndex.
                    map { case (newArgs, i) => AggFunction(name, distinct, newArgs, colNameInSample(alias, i)) }
                (splitAggregates, Set(alias))
              } else {
                (Seq(AggFunction(name, distinct, args, alias)), Set[String]())
              }
            }.unzip

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
        throw new RAException("Tuple-Bundler presently doesn't support LeftOuterJoin, Sort, or Limit")

    }
  }
}