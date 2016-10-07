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
        {//println("TYPE ID: "+t.id)
          Function("MIMIRCAST", List(apply(target), IntPrimitive(t.id)))}
      case Function("CAST", _) =>
        throw new SQLException("Invalid CAST: "+e)
      case _ => e.recur(apply(_: Expression))
    }
  }

  def apply(o: Operator): Operator = 
  {
    o match {

      /* 
       * SQLite ignores type information on tables.  This is kind of ugly, so
       * let's force it to behave by casting everything beforehand.
       */
      case table @ Table(tableName, columns, metadata) => {

        val args:List[ProjectArg] = columns.map((arg) => {
          println("Arg: " + arg._1 + " : " + arg._2)
          ProjectArg(arg._1,
              Function("MIMIRCAST",List( //Cast the field to ...
                Var(arg._1),
                IntPrimitive( arg._2.id )
              ))
          )
        }) ++ metadata.map( x => ProjectArg(x._1, Var(x._1)) )

        Project(args,table)
      }

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