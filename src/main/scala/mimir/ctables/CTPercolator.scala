package mimir.ctables

import java.sql.SQLException
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.algebra._
import mimir.util._
import mimir.optimizer._
import mimir.models.Model
import mimir.views.ViewAnnotation

object CTPercolator 
  extends LazyLogging
{

  val mimirRowDeterministicColumnName = "MIMIR_ROW_DET"
  val mimirColDeterministicColumnPrefix = "MIMIR_COL_DET_"

  private def extractMissingValueVar(expr: Expression): Var = {
    expr match {
      case Conditional(IsNullExpression(v1: Var), vg: VGTerm, v2: Var) =>
        if(v1 == v2) v1 else throw new SQLException("Unexpected clause to extractMisingValueVar")

      case _ => throw new SQLException("Unexpected clause to extractMisingValueVar")
    }
  }

  private def isMissingValueExpression(expr: Expression): Boolean = {
    expr match {
      case Conditional(IsNullExpression(var1: Var), vg: VGTerm, var2: Var) =>
        var1 == var2
      case _ => false
    }
  }

  def splitArith(expr: Expression): List[Expression] = {
    expr match {
      case Arithmetic(op, lhs, rhs) => splitArith(lhs) ++ splitArith(rhs)
      case x => List(x)
    }
  }

  val removeConstraintColumn = (oper: Operator) => {
    oper match {
      case Project(cols, src) =>
        Project(cols.filterNot((p) => p.name.equalsIgnoreCase(CTables.conditionColumn)), src)
      case _ =>
        oper
    }
  }

  def partition(oper: Project): Operator = {
    val cond = oper.columns.find(p => p.name.equalsIgnoreCase(CTables.conditionColumn)).head.expression
    var otherClausesOp: Operator = null
    var missingValueClausesOp: Operator = null
    var detExpr: List[Expression] = List()
    var nonDeterExpr: List[Expression] = List()


    val (missingValueClauses, otherClauses) =
      splitArith(cond).partition { (p) =>
        p match {
          case Comparison(_, lhs, rhs) => isMissingValueExpression(lhs) || isMissingValueExpression(rhs)
          case _ => false
        }
      }

    missingValueClauses.foreach{ (e) =>
      e match {
        case Comparison(op, lhs, rhs) =>
          var lhsExpr = lhs
          var rhsExpr = rhs

          if (isMissingValueExpression(lhs)) {
            val lhsVar = extractMissingValueVar(lhs)
            lhsExpr = lhsVar
            detExpr ++= List(Not(IsNullExpression(lhsVar)))
            nonDeterExpr ++= List(IsNullExpression(lhsVar))
          }
          if (isMissingValueExpression(rhs)) {
            val rhsVar = extractMissingValueVar(rhs)
            rhsExpr = rhsVar
            detExpr ++= List(Not(IsNullExpression(rhsVar)))
            nonDeterExpr ++= List(IsNullExpression(rhsVar))
          }

          detExpr ++= List(Comparison(op, lhsExpr, rhsExpr))

        case _ => throw new SQLException("Missing Value Clauses must be Comparison expressions")
      }
    }

    missingValueClausesOp = Union(
      removeConstraintColumn(oper).rebuild(List(Select(detExpr.distinct.reduce(ExpressionUtils.makeAnd(_, _)), oper.children().head))),
      oper.rebuild(List(Select(nonDeterExpr.distinct.reduce(ExpressionUtils.makeOr(_, _)), oper.children().head)))
    )

    if(otherClauses.nonEmpty)
      otherClausesOp = Project(
        oper.columns.filterNot( (p) => p.name.equalsIgnoreCase(CTables.conditionColumn))
          ++ List(ProjectArg(CTables.conditionColumn, otherClauses.reduce(ExpressionUtils.makeAnd(_, _)))),
        oper.source
      )

    (otherClausesOp, missingValueClausesOp) match {
      case (null, null) => throw new SQLException("Both partitions null")

      case (null, y) => y

      case (x, null) => x

      case (x, y) => Union(x, y)
    }
  }

  /**
   * Break up the conditions in the constraint column
   * into deterministic and non-deterministic fragments
   * ACCORDING to the data
   */
  def partitionConstraints(oper: Operator): Operator = {
    oper match {
      case Project(cols, src) =>
        cols.find(p => p.name.equalsIgnoreCase(CTables.conditionColumn)) match {
          case Some(arg) =>
            partition(oper.asInstanceOf[Project])

          case None =>
            oper
        }

      case _ =>
        oper
    }
  }

  /**
   * Rewrite the input operator to evaluate a 'provenance lite'
   * 
   * The operator's output will be extended with three types of columns:
   *   - a binary 'Row Deterministic' column that stores whether the row 
   *     is non-deterministically in the result set (true = deterministic).
   *   - a set of binary 'Column Deterministic' columns that store whether
   *     the value of the column is deterministic or not.
   *
   * The return value is a triple: The rewritten operator, an expression
   * for computing the non determinism of all columns, and an expression for
   * computing the non-determinism of all rows.
   */
  def percolateLite(oper: Operator, models: (String => Model)): (Operator, Map[String,Expression], Expression) =
  {
    oper match {
      case Project(columns, src) => {
        val (rewrittenSrc, colDeterminism, rowDeterminism) = percolateLite(src, models);

        logger.trace(s"PERCOLATE: $oper")
        logger.trace(s"GOT INPUT: $rewrittenSrc")

        // Compute the determinism of each column.
        val newColDeterminismBase = 
          columns.map( _ match { case ProjectArg(col, expr) => {
            val isDeterministic = 
              CTAnalyzer.compileDeterministic(expr, models, colDeterminism)
            
            (col, isDeterministic)
          }})

        logger.debug(s"PROJECT-BASE: $newColDeterminismBase")

        // Determine which of them are deterministic.
        val computedDeterminismCols = 
          newColDeterminismBase.filterNot( 
            // Retain only columns where the isDeterministic expression
            // is a constant (i.e., no Column references)
            { case (col, expr) => 
              ExpressionUtils.getColumns(expr).isEmpty
            }
          ).map( 
            // Then just translate to a list of ProjectArgs
            { case (col, isDeterministic) => 
              ProjectArg(mimirColDeterministicColumnPrefix+col, isDeterministic) 
            }
          )
        logger.debug(s"PROJECT-COLS: $computedDeterminismCols")

        // Rewrite these expressions so that the computed expressions use the
        // computed version from the source data.
        val newColDeterminism =
          newColDeterminismBase.map( _ match { case (col, isDeterministic) =>
              if(ExpressionUtils.getColumns(isDeterministic).isEmpty) {
                (col, isDeterministic)
              } else {
                //add entry to map nd col to its determinism decision maker
                (col, Var(mimirColDeterministicColumnPrefix+col))
              }
            }
          )

        val (newRowDeterminism, rowDeterminismCols) = 
          if(ExpressionUtils.getColumns(rowDeterminism).isEmpty){
            (rowDeterminism, List())
          } else {
            (Var(mimirRowDeterministicColumnName), 
              List(ProjectArg(
                mimirRowDeterministicColumnName, 
                rowDeterminism
            )))
          }

        //add the determinism metadata into the operator
        val retProject = Project(
            columns ++ computedDeterminismCols ++ rowDeterminismCols,
            rewrittenSrc
          )

        logger.debug(s"REWRITTEN: $retProject")

        return (retProject, newColDeterminism.toMap, newRowDeterminism)
      }
      case Aggregate(groupBy, aggregates, src) => {
        val (rewrittenSrc, colDeterminism, rowDeterminism) = percolateLite(src, models)

        // An aggregate value is is deterministic when...
        //  1. All of its inputs are deterministic (all columns referenced in the expr are det)
        //  2. All of its inputs are deterministically present (all rows in the group are det)

        // Start with the first case.  Come up with an expression to evaluate
        // whether the aggregate input is deterministic.
        val aggArgDeterminism: Seq[(String, Expression)] =
          aggregates.map((agg) => {
            val argDeterminism =
              agg.args.map(CTAnalyzer.compileDeterministic(_, models, colDeterminism))

            (agg.alias, ExpressionUtils.makeAnd(argDeterminism))
          })

        // Now come up with an expression to compute general row-level determinism
        val groupDeterminism: Expression =
          ExpressionUtils.makeAnd(
            rowDeterminism,
            ExpressionUtils.makeAnd(
              groupBy.map( group => colDeterminism(group.name) )
            )
          )

        // An aggregate is deterministic if the group is fully deterministic
        val aggFuncDeterminism: Seq[(String, Expression)] =
          aggArgDeterminism.map(arg => 
            (arg._1, ExpressionUtils.makeAnd(arg._2, groupDeterminism))
          )

        val (aggFuncMetaColumns, aggFuncMetaExpressions) = 
          aggFuncDeterminism.map({case (aggName, aggDetExpr) =>
            if(ExpressionUtils.isDataDependent(aggDetExpr)){
              ( 
                Some(AggFunction(
                  "GROUP_AND",
                  false,
                  List(aggDetExpr),
                  "MIMIR_AGG_DET_"+aggName
                )), 
                (aggName, Var("MIMIR_AGG_DET_"+aggName))
              )
            } else {
              (None, (aggName, aggDetExpr))
            }
          }).unzip

        // A group is deterministic if any of its group-by vars are
        val (groupMetaColumn, groupMetaExpression) =
          if(ExpressionUtils.isDataDependent(groupDeterminism)){
            (
              Some(AggFunction("GROUP_OR", false, List(groupDeterminism), "MIMIR_GROUP_DET")),
              Var("MIMIR_GROUP_DET")
            )
          } else {
            (None, groupDeterminism)
          }          

        // Assemble the aggregate function with metadata columns
        val extendedAggregate =
          Aggregate(
            groupBy, 
            aggregates ++ aggFuncMetaColumns.flatten ++ groupMetaColumn,
            rewrittenSrc
          )

        logger.debug(s"Aggregate Meta Columns: $groupMetaColumn // $aggFuncMetaColumns")
        logger.debug(s"Aggregate Determinism [Aggregates]: $aggFuncMetaExpressions")
        logger.debug(s"Aggregate Determinism [Row]: $groupMetaExpression")

        // Annotate all of the output columns
        val columnMetadata =
          aggFuncMetaExpressions ++
          groupBy.map( gb => (gb.name, BoolPrimitive(true)) )

        // Oliver says:
        // In this code, we're associating nondeterminism in the G.B. attributes with
        // the group's row, rather than with the G.B. cols.  There are some cases where
        // this isn't the intuitive thing to do (e.g., for the Type Inference Lens).
        // However, this is the more general approach, and so we're going to use it for
        // now --- Some user/expert studies down the line might be warranted.
        // 
        // Since this code is going to start migrating into GProM, we're going to want to 
        // start thinking of propagation rules in a more abstract Semiring style.

        // And return the new aggregate and annotations
        return (
          extendedAggregate,
          columnMetadata.toMap,
          groupMetaExpression
        )
      }

      
      case Annotate(subj,invisScm) => {
        percolateLite(subj, models)
      }
      
			case Recover(subj,invisScm) => {
        /*val provSelPrc =*/ percolateLite(subj, models)
        /*val detColsSeq = provSelPrc._2.toSeq
        val newDetCols = for ((projArg, (name,ctype), tableName) <- invisScm) yield {
          (name, CTAnalyzer.compileDeterministic(new Var(name), provSelPrc._2))
        }
       (oper, detColsSeq.union(newDetCols).toMap, provSelPrc._3)
          */
      }
      
      case ProvenanceOf(psel) => {
        val provSelPrc = percolateLite(psel, models)
        val provPrc = (new ProvenanceOf(provSelPrc._1), provSelPrc._2, provSelPrc._3)
        //GProMWrapper.inst.gpromRewriteQuery(sql);
        provPrc
      }
      
      case Select(cond, src) => {
        val (rewrittenSrc, colDeterminism, rowDeterminism) = percolateLite(src, models);

        // Compute the determinism of the selection predicate
        val condDeterminism = 
          CTAnalyzer.compileDeterministic(cond, models, colDeterminism)

        // Combine the determinism with the computed determinism from the child...
        val newRowDeterminism = 
          ExpressionUtils.makeAnd(condDeterminism, rowDeterminism)
        if( ExpressionUtils.getColumns(newRowDeterminism).isEmpty
            || condDeterminism.equals(BoolPrimitive(true))
          ){
          // If the predicate's determinism is data-independent... OR if the
          // predicate is deterministic already, then we don't need to add anything.
          // Just return what we already have!
          return (Select(cond, rewrittenSrc), colDeterminism, newRowDeterminism)
        } else {
          // Otherwise, we need to tack on a new projection operator to compute
          // the new non-determinism

          val projectArgs = 
            // remap the existing schema
            rewrittenSrc
              .columnNames
              // drop any existing row non-determinsm column
              .filterNot( _.equals(mimirRowDeterministicColumnName) )
              // pass the rest through unchanged
              .map( (x) => ProjectArg(x, Var(x))) ++
            List(ProjectArg(
              mimirRowDeterministicColumnName,
              newRowDeterminism
            ))

          val newOperator = 
            Project(projectArgs, Select(cond, rewrittenSrc) )

          return (newOperator, colDeterminism, Var(mimirRowDeterministicColumnName))
        }
      }
      case Union(left, right) => 
      {
        val (rewrittenLeft, colDetLeft, rowDetLeft) = percolateLite(left, models);
        val (rewrittenRight, colDetRight, rowDetRight) = percolateLite(right, models);
        val columnNames = colDetLeft.keys.toSet ++ colDetRight.keys.toSet
        // We need to make the schemas line up: the schemas of the left and right
        // need to have all of the relevant columns
        // Going to cheat a bit here and just force the projection on.

        val mergeNonDeterminism = 
          (col:String, detCol:String, colLeft:Expression, colRight:Expression) => {
          // It's only safe to skip the non-determinism column if both
          // LHS and RHS have exactly the same condition AND that condition
          // is data-independent.
          if(  ExpressionUtils.getColumns(colLeft).isEmpty
            && ExpressionUtils.getColumns(colRight).isEmpty
            && colLeft.equals(colRight)
          ){
            // Skip the column and return the data-independent condition
            (List[(ProjectArg, ProjectArg)](), (col, colLeft))
          } else {
            // Otherwise, materialize the condition, and return a reference
            // to the new condition.
            ( List( (ProjectArg(detCol, colLeft), 
                     ProjectArg(detCol, colRight) )), 
              (col, Var(detCol))
            )
          }
        }

        val (colProjectArgs, colDeterminism) =
          columnNames.map(
            (col) => 
              mergeNonDeterminism(
                col, mimirColDeterministicColumnPrefix+col, 
                colDetLeft(col), colDetRight(col)
              )
          ).unzip

        val (condProjectArgs, condDeterminism) =
          mergeNonDeterminism(
            null, mimirRowDeterministicColumnName,
            rowDetLeft, rowDetRight
          )
        
        val (projectArgsLeft, projectArgsRight) = 
          (colProjectArgs.flatten ++ condProjectArgs).unzip
        val projectArgsBase = 
          columnNames.map( 
            (col) => ProjectArg(col, Var(col)) 
          ).toList

        val newOperator = 
          Union(
              Project(projectArgsBase ++ projectArgsLeft, rewrittenLeft),
              Project(projectArgsBase ++ projectArgsRight, rewrittenRight)
            )

        return (newOperator, colDeterminism.toMap, condDeterminism._2)

      }
      case Join(left, right) =>
      {
        val (rewrittenLeft, colDetLeft, rowDetLeft) = percolateLite(left, models);
        val (rewrittenRight, colDetRight, rowDetRight) = percolateLite(right, models);
        logger.trace(s"JOIN:\n$left\n$right")
        logger.trace(s"INTO:\n$rewrittenLeft\n$rewrittenRight")
        // if left and right have no schema overlap, then the only
        // possible overlap in rewrittenLeft and rewrittenRight is
        // the row determinism column.  Start by detecting whether
        // this column is present in both inputs:
        val (schemaLeft,detColumnLeft) = 
          rewrittenLeft
            .columnNames
            .partition( _ != mimirRowDeterministicColumnName )
        val (schemaRight,detColumnRight) = 
          rewrittenRight
            .columnNames
            .partition( _ != mimirRowDeterministicColumnName )

        // If either left or right side lacks a determinism column,
        // we're safe.  Fast-path return a simple join.
        if(detColumnLeft.isEmpty || detColumnRight.isEmpty){
          return (
            Join(rewrittenLeft, rewrittenRight),
            colDetLeft ++ colDetRight,
            ExpressionUtils.makeAnd(rowDetLeft, rowDetRight)
          )          
        }

        // if both left and right have a row determinism column,
        // then we need to rewrite them to prevent a namespace
        // collision.

        // Generate a schema mapping that leaves normal columns
        // intact.
        val schemaMappingLeft = 
          schemaLeft.map( (x) => ProjectArg(x, Var(x))) ++ 
          (detColumnLeft.map( 
            (_) => ProjectArg(
                mimirRowDeterministicColumnName+"_LEFT",
                Var(mimirRowDeterministicColumnName)
              ))
          )
        val schemaMappingRight = 
          schemaRight.map( (x) => ProjectArg(x, Var(x))) ++ 
          (detColumnRight.map( 
            (_) => ProjectArg(
                mimirRowDeterministicColumnName+"_RIGHT",
                Var(mimirRowDeterministicColumnName)
              ))
          )

        // Map the variables in the determinism columns...
        val mappedRowDetLeft = Eval.inline(rowDetLeft, 
            Map((mimirRowDeterministicColumnName, 
                 Var(mimirRowDeterministicColumnName+"_LEFT"))))
        val mappedRowDetRight = Eval.inline(rowDetRight, 
            Map((mimirRowDeterministicColumnName, 
                 Var(mimirRowDeterministicColumnName+"_RIGHT"))))

        // And return it.
        return (
          Join(
            Project(schemaMappingLeft, rewrittenLeft), 
            Project(schemaMappingRight, rewrittenRight)
          ),
          colDetLeft ++ colDetRight,
          ExpressionUtils.makeAnd(mappedRowDetLeft, mappedRowDetRight)
        )
      }
      case Table(name, alias, cols, metadata) => {
        return (oper, 
          // All columns are deterministic
          cols.map(_._1).map((_, BoolPrimitive(true)) ).toMap,
          // All rows are deterministic
          BoolPrimitive(true)
        )
      }
      case v @ View(name, query, metadata) => {
        val (newQuery, colDeterminism, rowDeterminism) = percolateLite(query, models)
        val columns = query.columnNames

        val inlinedQuery = 
          Project(
            columns.map { col => ProjectArg(col, Var(col)) } ++
            columns.map { col => ProjectArg(mimirColDeterministicColumnPrefix+col, colDeterminism(col)) } ++
            Seq( ProjectArg(mimirRowDeterministicColumnName, rowDeterminism) ),
            newQuery
          )
        (
          View(name, inlinedQuery, metadata + ViewAnnotation.TAINT),
          columns.map { col => (col -> Var(mimirColDeterministicColumnPrefix+col)) }.toMap,
          Var(mimirRowDeterministicColumnName)
        )
      }
      case v @ AdaptiveView(model, name, query, metadata) => { 
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
        val (newQuery, colDeterminism, rowDeterminism) = percolateLite(query, models)
        val columns = query.columnNames

        val inlinedQuery = 
          Project(
            columns.map { col => ProjectArg(col, Var(col)) } ++
            columns.map { col => ProjectArg(mimirColDeterministicColumnPrefix+col, colDeterminism(col)) } ++
            Seq( ProjectArg(mimirRowDeterministicColumnName, rowDeterminism) ),
            newQuery
          )
        (
          AdaptiveView(model, name, inlinedQuery, metadata + ViewAnnotation.TAINT),
          columns.map { col => (col -> Var(mimirColDeterministicColumnPrefix+col)) }.toMap,
          Var(mimirRowDeterministicColumnName)
        )
      }

      case HardTable(sch,_) => {
        return (oper, 
          // All columns are deterministic
          sch.map(_._1).map((_, BoolPrimitive(true)) ).toMap,
          // All rows are deterministic
          BoolPrimitive(true)
        )
      }
 

      // This is a bit hackish... Sort alone doesn't affect determinism
      // metadata, and Limit doesn't either, but combine the two and you get some
      // annoying behavior.  Since we're rewriting this particular fragment soon, 
      // I'm going to hold off on any elegant solutions
      case Sort(sortCols, src) => {
        val (rewritten, cols, row) = percolateLite(src, models)
        (Sort(sortCols, rewritten), cols, row)
      }
      case Limit(offset, count, src) => {
        val (rewritten, cols, row) = percolateLite(src, models)
        (Limit(offset, count, rewritten), cols, row)
      }

      case _:LeftOuterJoin =>
        throw new RAException("Don't know how to percolate a left-outer-join")
    }
  }
  
  def percolateGProM(oper: Operator): (Operator, Map[String,Expression], Expression) =
  {
    mimir.algebra.gprom.OperatorTranslation.compileTaintWithGProM(oper)
  }
}
