package mimir.provenance

import java.sql.SQLException

import mimir.algebra._
import mimir.util._
import mimir.optimizer._

class ProvenanceError(e:String) extends Exception(e) {}

object Provenance {

  def rowidColnameBase = "MIMIR_ROWID"

  def compile(oper: Operator): (Operator, List[String]) = {
    val makeRowIDProjectArgs = 
      (rowids: List[String], offset: Integer, padLen: Integer) => {
        rowids.map(Var(_)).
               padTo(padLen, RowIdPrimitive("-")).
               zipWithIndex.map( { case (v, i) => 
                  val newName = rowidColnameBase + "_" + (i+offset)
                  (newName, ProjectArg(newName, v))
               }).
               unzip
    }
    oper match {
      case Project(args, src) => {
        val (newSrc, rowids) = compile(src)
        val newArgs = 
          args.map( arg => 
            ProjectArg(arg.name, inline(arg.expression, rowids))
          )
        val (newRowids, rowIDProjections) = makeRowIDProjectArgs(rowids, 0, 0)
        (
          Project(newArgs ++ rowIDProjections, newSrc),
          newRowids
        )
      }

      case Select(cond, src) => {
        val (newSrc, rowids) = compile(src)
        ( 
          Select(inline(cond, rowids), newSrc), 
          rowids
        )
      }

      case Join(lhs, rhs) => {
        val (newLhs, lhsRowids) = compile(lhs)
        val (newRhs, rhsRowids) = compile(rhs)
        val (newLhsRowids, lhsIdProjections) = 
          makeRowIDProjectArgs(lhsRowids, 0, 0)
        val (newRhsRowids, rhsIdProjections) = 
          makeRowIDProjectArgs(rhsRowids, lhsRowids.size, 0)
        val lhsProjectArgs =
          lhs.schema.map(x => ProjectArg(x._1, Var(x._1))) ++ lhsIdProjections
        val rhsProjectArgs = 
          rhs.schema.map(x => ProjectArg(x._1, Var(x._1))) ++ rhsIdProjections
        (
          Join(
            Project(lhsProjectArgs, newLhs),
            Project(rhsProjectArgs, newRhs)
          ),
          newLhsRowids ++ newRhsRowids
        )
      }

      case Union(lhs, rhs) => {
        val (newLhs, lhsRowids) = compile(lhs)
        val (newRhs, rhsRowids) = compile(rhs)
        val (newRowids, lhsIdProjections) = 
          makeRowIDProjectArgs(lhsRowids, 0, rhsRowids.size)
        val (_,         rhsIdProjections) = 
          makeRowIDProjectArgs(rhsRowids, 0, lhsRowids.size)
        val lhsProjectArgs =
          lhs.schema.map(x => ProjectArg(x._1, Var(x._1))) ++ 
            lhsIdProjections ++ 
            List(ProjectArg(rowidColnameBase+"_branch", RowIdPrimitive("left")))
        val rhsProjectArgs = 
          rhs.schema.map(x => ProjectArg(x._1, Var(x._1))) ++ 
            rhsIdProjections ++ 
            List(ProjectArg(rowidColnameBase+"_branch", RowIdPrimitive("right")))
        (
          Union(
            Project(lhsProjectArgs, newLhs),
            Project(rhsProjectArgs, newRhs)
          ),
          newRowids
        )
      }

      case Table(name, schema, meta) =>
        (
          Table(name, schema, meta ++ List((rowidColnameBase, Var("ROWID"), Type.TRowId))),
          List(rowidColnameBase)
        )

    }
  }

  def rowIdVal(rowids: List[Expression]): Expression =
    Function("MIMIR_MAKE_ROWID", rowids)    

  def rowIdVar(rowids: List[String]): Expression = 
    rowIdVal(rowids.map(Var(_)))

  def inline(expr: Expression, rowids: List[String]): Expression = {
    expr match {
      case RowIdVar() => rowIdVar(rowids)
      case _ => expr.rebuild(expr.children.map(inline(_,rowids)))
    }
  }
}