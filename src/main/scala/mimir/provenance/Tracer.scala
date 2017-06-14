package mimir.provenance

import java.sql.SQLException
import mimir.algebra._

class TracerInvalidPath() extends Exception {}

object Tracer {

  def trace(oper: Operator, targetRowId: Map[String, RowIdPrimitive]): 
      (Operator, Map[String,Expression], Expression) =
  {
    oper match {
      case p @ Project(args, src) => {
        val srcTargetRowIds = 
          targetRowId.flatMap({ case (rowIdColKey, rowIdColValue) => 
            p.get(rowIdColKey) match {
              case Some(Var(newColName)) => Some((newColName, rowIdColValue))
              case Some(rowid:RowIdPrimitive) => 
                if(rowid.equals(rowIdColValue)) { None }
                else { throw new TracerInvalidPath() }
              case _ => throw new SQLException("BUG: Expecting traced expression to have a rowId column that it doesn't have")
            }
          })
        val (retOper, retExprs, retCondition) = trace(src, srcTargetRowIds)
        (
          retOper,
          args.map( arg => 
            (arg.name, Eval.inline(arg.expression, retExprs))
          ).toMap,
          Eval.inline(retCondition, retExprs)
        )
      }

      case Select(cond, src) => {
        val (retOper, retExprs, retCondition) = trace(src, targetRowId)
        (
          retOper,
          retExprs,
          ExpressionUtils.makeAnd(retCondition, cond)
        )
      }

      case Join(lhs, rhs) => {
        val lhsBaseSchema = lhs.schema.map(_._1).toSet
        val (lhsTargetRowId, rhsTargetRowId) = 
          targetRowId.partition( x => lhsBaseSchema.contains(x._1) )

        val (lhsRetOper, lhsRetExprs, lhsRetCondition) = trace(lhs, lhsTargetRowId)
        val (rhsRetOper, rhsRetExprs, rhsRetCondition) = trace(rhs, rhsTargetRowId)

        val lhsSchema = lhsRetOper.schema.map(_._1).toSet
        val rhsSchema = rhsRetOper.schema.map(_._1).toSet
        val overlappingSchema = lhsSchema & rhsSchema

        if(overlappingSchema.isEmpty){
          (
            Join(lhsRetOper, rhsRetOper),
            lhsRetExprs ++ rhsRetExprs,
            ExpressionUtils.makeAnd(lhsRetCondition, rhsRetCondition)
          )
        } else {
          val lhsSafeSchema = lhsSchema -- overlappingSchema
          val rhsSafeSchema = rhsSchema -- overlappingSchema
          val lhsReplacements = 
            overlappingSchema.map( x => (x, Var(x+"_left"))).toMap
          val rhsReplacements = 
            overlappingSchema.map( x => (x, Var(x+"_right"))).toMap
          (
            Join(
              Project(
                lhsSafeSchema.map( x => ProjectArg(x, Var(x))).toList++
                  overlappingSchema.map( x => ProjectArg(x+"_left", Var(x))),
                lhsRetOper
              ),
              Project(
                rhsSafeSchema.map( x => ProjectArg(x, Var(x))).toList++
                  overlappingSchema.map( x => ProjectArg(x+"_right", Var(x))),
                rhsRetOper
              )
            ),
            (
              lhsRetExprs.map(
                x => (x._1,
                  Eval.inlineWithoutSimplifying(x._2, lhsReplacements)
                )
              ) ++
              rhsRetExprs.map(
                x => (x._1,
                  Eval.inlineWithoutSimplifying(x._2, rhsReplacements)
                )
              )
            ),
            ExpressionUtils.makeAnd(
              Eval.inlineWithoutSimplifying(lhsRetCondition, lhsReplacements),
              Eval.inlineWithoutSimplifying(rhsRetCondition, rhsReplacements)
            )
          )
        }
      }

      case Union(lhs,rhs) =>
        try { 
          trace(lhs, targetRowId)
        } catch {
          case _:TracerInvalidPath => 
            trace(rhs, targetRowId)
        }

      case View(_, query, _) => 
        trace(query, targetRowId)

      case Table(name, alias, schema, meta) =>
        val targetFilter = 
          ExpressionUtils.makeAnd(
            targetRowId.toList.map({ case (rowIdColKey, rowIdColValue) => 
              Comparison(Cmp.Eq, Var(rowIdColKey), rowIdColValue)
            })
          )

        (
          Select(targetFilter, oper), 
          (schema.map(_._1)++meta.map(_._1)).map(
            col => (col, Var(col))
          ).toMap,
          BoolPrimitive(true)
        )

      case EmptyTable(schema) => 
        ( 
          EmptyTable(schema),
          schema.map(_._1).map(
            col => (col, Var(col))
          ).toMap,
          BoolPrimitive(true)
        )

      case Sort(_, src) => return trace(oper, targetRowId)
      case Limit(_, _, src) => return trace(oper, targetRowId)

      case _:LeftOuterJoin => 
        throw new RAException("Tracer can't handle left outer joins")

      case _:Aggregate => 
        throw new RAException("Tracer can't handle aggregates")

      case (Annotate(_, _) | ProvenanceOf(_) | Recover(_, _)) => ???
    }
  }
}
