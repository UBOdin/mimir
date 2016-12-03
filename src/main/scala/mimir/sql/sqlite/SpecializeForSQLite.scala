package mimir.sql.sqlite;

import java.sql._;

import mimir.algebra._;
import mimir.ctables._;
import mimir.util._;

object SpecializeForSQLite {

  def apply(e: Expression): Expression =
  {
    e match {
      case Function("CAST", List(target, TypePrimitive(t))) => 
        {//println("TYPE ID: "+t.id(t))
          Function("MIMIRCAST", List(apply(target), IntPrimitive(t.id(t))))}
      case Function("CAST", _) =>
        throw new SQLException("Invalid CAST: "+e)
      case _ => e.recur(apply(_: Expression))
    }
  }

  def apply(o: Operator): Operator = 
  {
    o match {

      /*
       * Rewrite Expressions to replace SQLite's built in CAST 
       * operation, which masks failures with default types, 
       * with our own.
       */
      case _ => 
        o.recurExpressions( apply(_:Expression) ).
          recur( apply(_:Operator) )

    }
  }

}