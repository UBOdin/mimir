package mimir.provenance

import mimir.algebra._

object Tracer {

  def trace(oper: Operator): (Operator, Map[String,Expression], Expression) =
    oper match {
      case Project(args, src) => {
        val (retOper, retExprs, retCondition) = trace(src)
        (
          retOper,
          args.map( arg => 
            (arg.name, Eval.inline(arg.expression, retExprs))
          ).toMap,
          Eval.inline(retCondition, retExprs)
        )
      }

      case Select(cond, src) => {
        val (retOper, retExprs, retCondition) = trace(src)
        (
          Select(Eval.inline(cond, retExprs), retOper),
          retExprs,
          ExpressionUtils.makeAnd(retCondition, cond)
        )
      }

      case Join(lhs, rhs) => {
        val (lhsRetOper, lhsRetExprs, lhsRetCondition) = trace(lhs)
        val (rhsRetOper, rhsRetExprs, rhsRetCondition) = trace(rhs)

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

      case Union(_,_) =>
        throw new ProvenanceError("Tracing an operator that has not yet been filtered for a specific token")

      case Table(name, schema, meta) =>
        (
          oper, 
          (schema.map(_._1)++meta.map(_._1)).map(
            col => (col, Var(col))
          ).toMap,
          BoolPrimitive(true)
        )
    }

}