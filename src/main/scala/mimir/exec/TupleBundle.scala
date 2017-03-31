package mimir.exec

import mimir.Database
import mimir.algebra._
import mimir.ctables._

class TupleBundler(db: Database, sampleSeeds: Seq[Int] = (0 until 10))
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
        )}, 
      compiled
    )
  }

  def splitExpressionIfNeeded(expression: Expression, nonDeterministicInputs: Set[String]): (Option[Seq[Expression]]) =
  {
    val allInputs = ExpressionUtils.getColumns(expression)
    val expressionHasANonDeterministicInput =
      allInputs.exists { nonDeterministicInputs(_) }
    val expressionIsNonDeterministic =
      !CTables.isDeterministic(expression)

    if(expressionHasANonDeterministicInput || expressionIsNonDeterministic){
      val outputColumns =
        sampleSeeds.zipWithIndex.map { case (seed, i) => 
          val inputInstancesInThisSample = 
            nonDeterministicInputs.
              map { x => (x -> Var(colNameInSample(x, i)) ) }.
              toMap
          val expressionInThisSample =
            CTAnalyzer.compileSample(
              Eval.inline(expression, inputInstancesInThisSample),
              IntPrimitive(seed)
            )

          expressionInThisSample
        }

      Some(outputColumns)
    } else { None }
  }

  def compileFlat(query: Operator): (Operator, Set[String]) =
  {
    query match {
      case t: Table => 
        (
          OperatorUtils.projectInColumn(
            "MIMIR_WORLD_BITS", IntPrimitive(fullBitVector), 
            t
          ),
          t.schema.map(_._1).toSet
        )

      case Project(columns, oldChild) => {
        val (newChild, nonDeterministicInput) = compileFlat(oldChild)

        val (
          newColumns,
          nonDeterministicOutputs
        ):(Seq[Seq[ProjectArg]], Seq[Set[String]]) = columns.map { col => 
            splitExpressionIfNeeded(
              col.expression, 
              nonDeterministicInput
            ) match {
              case None => (Seq(col), Set[String]())
              case Some(replacements) => ( 
                replacements.zipWithIndex.map { case (expr, i) => ProjectArg(colNameInSample(col.name, i), expr) },
                Set(col.name)
              )
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

        splitExpressionIfNeeded(condition, nonDeterministicInput) match {
          case None => ( Select(condition, newChild), nonDeterministicInput )

          case Some(replacements) => 
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
        }
      }

      // case Aggregate(gbColumns, aggColumns, oldChild) => {
      //   val (newChild, nonDeterministicInput) = compileFlat(oldChild)

      //   val oneOfTheGroupByColumnsIsNonDeterministic =
      //     gbColumns.map(_.name).exists(nonDeterministicInput(_))

      //   if(oneOfTheGroupByColumnsIsNonDeterministic){

          

      //   }


      // }

    }
  }
}