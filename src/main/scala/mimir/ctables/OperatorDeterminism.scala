package mimir.ctables

import java.sql.SQLException
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.algebra._
import mimir.util._
import mimir.optimizer._
import mimir.models.Model
import mimir.views.ViewAnnotation

object OperatorDeterminism 
  extends LazyLogging
{

  val mimirRowDeterministicColumnName = ID("MIMIR_ROW_DET")
  val mimirColDeterministicColumnPrefix = "MIMIR_COL_DET_"
  def mimirColDeterministicColumn(col:ID):ID = ID(mimirColDeterministicColumnPrefix,col)

  def splitArith(expr: Expression): List[Expression] = {
    expr match {
      case Arithmetic(op, lhs, rhs) => splitArith(lhs) ++ splitArith(rhs)
      case x => List(x)
    }
  }

  val removeConstraintColumn = (oper: Operator) => {
    oper match {
      case Project(cols, src) =>
        Project(
          cols.filterNot { 
            p => p.name.equals(CTables.conditionColumn)
          }, 
          src
        )
      case _ =>
        oper
    }
  }

  // def partition(oper: Project): Operator = {
  //   val cond = oper.get(CTables.conditionColumn).get
  //   var otherClausesOp: Operator = null
  //   var missingValueClausesOp: Operator = null
  //   var detExpr: List[Expression] = List()
  //   var nonDeterExpr: List[Expression] = List()


  //   val (missingValueClauses, otherClauses) =
  //     splitArith(cond).partition { (p) =>
  //       p match {
  //         case Comparison(_, lhs, rhs) => isMissingValueExpression(lhs) || isMissingValueExpression(rhs)
  //         case _ => false
  //       }
  //     }

  //   missingValueClauses.foreach{ (e) =>
  //     e match {
  //       case Comparison(op, lhs, rhs) =>
  //         var lhsExpr = lhs
  //         var rhsExpr = rhs

  //         if (isMissingValueExpression(lhs)) {
  //           val lhsVar = extractMissingValueVar(lhs)
  //           lhsExpr = lhsVar
  //           detExpr ++= List(Not(IsNullExpression(lhsVar)))
  //           nonDeterExpr ++= List(IsNullExpression(lhsVar))
  //         }
  //         if (isMissingValueExpression(rhs)) {
  //           val rhsVar = extractMissingValueVar(rhs)
  //           rhsExpr = rhsVar
  //           detExpr ++= List(Not(IsNullExpression(rhsVar)))
  //           nonDeterExpr ++= List(IsNullExpression(rhsVar))
  //         }

  //         detExpr ++= List(Comparison(op, lhsExpr, rhsExpr))

  //       case _ => throw new SQLException("Missing Value Clauses must be Comparison expressions")
  //     }
  //   }

  //   missingValueClausesOp = Union(
  //     removeConstraintColumn(oper).rebuild(List(Select(detExpr.distinct.reduce(ExpressionUtils.makeAnd(_, _)), oper.children().head))),
  //     oper.rebuild(List(Select(nonDeterExpr.distinct.reduce(ExpressionUtils.makeOr(_, _)), oper.children().head)))
  //   )

  //   if(otherClauses.nonEmpty)
  //     otherClausesOp = Project(
  //       oper.columns.filterNot { 
  //         p => p.name.equals(CTables.conditionColumn)
  //       } ++ List(
  //         ProjectArg(
  //           CTables.conditionColumn, 
  //           otherClauses.reduce(
  //             ExpressionUtils.makeAnd(_, _)
  //           )
  //         )
  //       ),
  //       oper.source
  //     )

  //   (otherClausesOp, missingValueClausesOp) match {
  //     case (null, null) => throw new SQLException("Both partitions null")

  //     case (null, y) => y

  //     case (x, null) => x

  //     case (x, y) => Union(x, y)
  //   }
  // }

  /**
   * Break up the conditions in the constraint column
   * into deterministic and non-deterministic fragments
   * ACCORDING to the data
   */
  // def partitionConstraints(oper: Operator): Operator = {
  //   oper match {
  //     case proj@Project(cols, src) 
  //           if cols.exists { _.name.equals(CTables.conditionColumn) }
  //         => partition(proj)

  //     case _ =>
  //       oper
  //   }
  // }

  /**
   * Rewrite the input operator to evaluate a 'provenance lite'
   * 
   * For an input with N columns, the output will include 2N+1 columns.
   * In addition to the normal columns, there will be.
   *   - a binary 'Row Deterministic' column that stores whether the row 
   *     is non-deterministically in the result set (true = deterministic).
   *   - a set of binary 'Column Deterministic' columns that store whether
   *     the value of the column is deterministic or not.
   * The return value is just the rewritten operator itself.  Determinism
   * column names can be inferred from the pre-operator schema.
   */
  def compile(oper: Operator, models: (ID => Model)): Operator =
  {
    oper match {
      case Project(columns, src) => {
        val rewritten = compile(src, models)

        logger.trace(s"PERCOLATE: $oper")
        logger.trace(s"GOT INPUT: $rewritten")

        // Compute the determinism of each column.
        val inputColDeterminism = 
          src.columnNames.map { col => 
            col -> Var(mimirColDeterministicColumn(col)) 
          }.toMap
        val outputColDeterminism = 
          columns.map( _ match { case ProjectArg(col, expr) => {
            val isDeterministic = 
              ExpressionDeterminism.compileDeterministic(expr, inputColDeterminism)
            
            ProjectArg(mimirColDeterministicColumn(col), isDeterministic)
          }})
        val outputRowDeterminism =
          ProjectArg(mimirRowDeterministicColumnName, Var(mimirRowDeterministicColumnName))

        logger.debug(s"PROJECT-DETERMINISM: $outputColDeterminism")

        val ret = Project(
          (columns ++ outputColDeterminism) :+ outputRowDeterminism,
          rewritten
        )
        logger.debug(s"PROJECT-REWRITTEN: $ret")

        ret
      }

      case Aggregate(groupBy, aggregates, src) => {
        val rewritten = compile(src, models)

        // An aggregate value is is deterministic when...
        //  1. All of its inputs are deterministic (all columns referenced in the expr are det)
        //  2. All of its inputs are deterministically present (all rows in the group are det)

        val inputColDeterminism = 
          src.columnNames.map { col => 
            col -> Var(mimirColDeterministicColumn(col)) 
          }.toMap

        // Start with the first case.  Come up with an expression to evaluate
        // whether the aggregate input is deterministic.
        val aggArgDeterminism: Seq[(ID, Expression)] =
          aggregates.map  { agg => 
            val argDeterminism =
              agg.args.map { ExpressionDeterminism.compileDeterministic(_, inputColDeterminism) }

            (agg.alias, ExpressionUtils.makeAnd(argDeterminism))
          }

        // Now come up with an expression to compute general row-level determinism
        val groupDeterminism: Expression =
          ExpressionUtils.makeAnd(
            Var(mimirRowDeterministicColumnName),
            ExpressionUtils.makeAnd(
              groupBy.map{ group => Var(mimirColDeterministicColumn(group.name)) }
            )
          )

        // An aggregate is deterministic if the group is fully deterministic
        val aggFuncDeterminism: Seq[(ID, Expression)] =
          aggArgDeterminism.map(arg => 
            (arg._1, ExpressionUtils.makeAnd(arg._2, groupDeterminism))
          )

        val aggFuncMetaColumns = 
          aggFuncDeterminism.map { case (aggName, aggDetExpr) =>
            AggFunction(
              ID("group_and"),
              false,
              Seq(aggDetExpr),
              mimirColDeterministicColumn(aggName)
            )
          }

        val groupByMetaColumns =
          groupBy.map { group => 
            AggFunction(
              ID("group_and"),
              false,
              Seq(BoolPrimitive(true)),
              mimirColDeterministicColumn(group.name)
            )
          }

        // A group is deterministic if any of its group-by vars are
        val groupMetaColumn =
          AggFunction(
            ID("group_and"),
            false,
            Seq(groupDeterminism),
            mimirRowDeterministicColumnName
          )

        // Assemble the aggregate function with metadata columns
        val extendedAggregate =
          Aggregate(
            groupBy, 
            (aggregates ++ groupByMetaColumns ++ aggFuncMetaColumns) :+ groupMetaColumn,
            rewritten
          )

        logger.debug(s"Aggregate Meta Columns: $groupMetaColumn // $aggFuncMetaColumns")

        return extendedAggregate
      }

      case Select(cond, src) => {
        val rewritten = compile(src, models);

        val inputColDeterminism = 
          src.columnNames.map { col => 
            col -> Var(mimirColDeterministicColumn(col)) 
          }.toMap

        // Compute the determinism of the selection predicate
        val condDeterminism = 
          ExpressionDeterminism.compileDeterministic(cond, inputColDeterminism)


        Select(cond, rewritten)
          .alterColumnsByID(mimirRowDeterministicColumnName -> 
            ExpressionUtils.makeAnd(
              Var(mimirRowDeterministicColumnName),
              condDeterminism
            )
          )
        
      }
      case Union(left, right) => 
      {
        return Union(
          compile(left, models),
          compile(right, models)
        )
      }
      case Join(left, right) =>
      {
        val rewrittenLeft = compile(left, models);
        val rewrittenRight = compile(right, models);
        logger.trace(s"JOIN:\n$left\n$right")
        logger.trace(s"INTO:\n$rewrittenLeft\n$rewrittenRight")

        val leftCol = ID(mimirRowDeterministicColumnName,"_LEFT")
        val rightCol = ID(mimirRowDeterministicColumnName,"_RIGHT")

        Join(
          rewrittenLeft.renameByID( mimirRowDeterministicColumnName -> leftCol ),
          rewrittenRight.renameByID( mimirRowDeterministicColumnName -> rightCol )
        ).addColumnsByID( 
          mimirRowDeterministicColumnName -> 
            ExpressionUtils.makeAnd(Var(leftCol), Var(rightCol)) 
        ).removeColumnsByID(leftCol, rightCol)
      }
      case _:Table | _:HardTable => {
        val colDeterminism = 
          oper.columnNames.map { col => mimirColDeterministicColumn(col) -> BoolPrimitive(true) }
        val rowDeterminism = 
          mimirRowDeterministicColumnName -> BoolPrimitive(true)
        val allDeterminmism = colDeterminism :+ rowDeterminism
        
        return oper.addColumnsByID(allDeterminmism:_*)
      }
      case View(name, query, metadata) => {
        View(
          name,
          compile(query, models),
          metadata + ViewAnnotation.TAINT
        )
      }
      case LensView(lens, name, query, metadata) => { 
        // Oliver:
        //   I'm on the fence about whether we want to treat this case as being different from default views.
        //   In principle, we want to decide whether any of the attributes or the table itself are NonDet and
        //   to tag the relevant columns/rows here.  However, this has the side effect of tagging *every* row 
        //   with non-determinsm.  However, as described in issue #165
        //    > https://github.com/UBOdin/mimir/issues/165
        //   this may not be the interface that we're looking for.  Table-wide annotations should *really* 
        //   be added through some other vector (e.g., labeling column headers or somesuch)
        //
        //   For now, I'm just going to leave adaptive views to operate like they would otherwise operate.
        //   As soon as it becomes appropriate to start tagging things... then see 
        //   CTExplainer.explainSubsetWithoutOptimizing for an idea of how to implement this correctly.
        LensView(
          lens, 
          name, 
          compile(query, models), 
          metadata + ViewAnnotation.TAINT
        )
      }
 

      // This is a bit hackish... Sort alone doesn't affect determinism
      // metadata, and Limit doesn't either, but combine the two and you get some
      // annoying behavior.  Since we're rewriting this particular fragment soon, 
      // I'm going to hold off on any elegant solutions
      case Sort(sortCols, src) => 
        Sort(sortCols, compile(src, models))
      case Limit(offset, count, src) => 
        Limit(offset, count, compile(src, models))

      // This is also a bit hackish.  We're not accounting for uncertain
      // tuples on the RHS.  TODO: fixme
      case LeftOuterJoin(lhs, rhs, cond) => {
        val rewrittenLeft = compile(lhs, models);
        val rewrittenRight = compile(rhs, models);
        logger.trace(s"JOIN:\n$lhs\n$rhs")
        logger.trace(s"INTO:\n$rewrittenLeft\n$rewrittenRight")

        val leftCol = ID(mimirRowDeterministicColumnName,"_LEFT")
        val rightCol = ID(mimirRowDeterministicColumnName,"_RIGHT")

        val inputColDeterminism = 
          (lhs.columnNames ++ rhs.columnNames).map { col => 
            col -> Var(mimirColDeterministicColumn(col)) 
          }.toMap

        // Compute the determinism of the selection predicate
        val condDeterminism = 
          ExpressionDeterminism.compileDeterministic(cond, inputColDeterminism)

        LeftOuterJoin(
          rewrittenLeft.renameByID( mimirRowDeterministicColumnName -> leftCol ),
          rewrittenRight.renameByID( mimirRowDeterministicColumnName -> rightCol ),
          cond
        ).addColumnsByID( 
          mimirRowDeterministicColumnName -> 
            ExpressionUtils.makeAnd(Seq(
              Var(leftCol), 
              Var(rightCol),
              condDeterminism
            )) 
        ).removeColumnsByID(leftCol, rightCol)}
    }
  }
  
  def columnNames(oper:Operator) : Seq[ID] = {
    oper match {
      case Table(_,_,sch,_) => sch.map(_._1)
      case _ => oper.columnNames
    }
  }
}
