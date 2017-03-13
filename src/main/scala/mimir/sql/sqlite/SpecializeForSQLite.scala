package mimir.sql.sqlite;

import java.sql._;

import mimir.algebra._;
import mimir.ctables._;
import mimir.util._;

object SpecializeForSQLite {

  def apply(e: Expression, schema: Map[String, Type]): Expression =
  {
    (e match {
  
      case Function("CAST", List(target, TypePrimitive(t))) => 
        {//println("TYPE ID: "+t.id(t))
          Function("MIMIRCAST", List(target, IntPrimitive(Type.id(t))))}

      case Function("CAST", _) =>
        throw new SQLException("Invalid CAST: "+e)

      case Function("FIRST", Seq(arg)) =>
        Typechecker.typeOf(arg, schema) match {
          case TInt()   => Function("FIRST_INT", Seq(arg))
          case TFloat() => Function("FIRST_FLOAT", Seq(arg))
          case _        => Function("FIRST", Seq(arg))
        }

      case _ => e

    }).recur( apply(_, schema) )
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
        val schema = o.schema.toMap
        o.recurExpressions( apply(_:Expression, schema) ).
          recur( apply(_:Operator) )

    }
  }

}