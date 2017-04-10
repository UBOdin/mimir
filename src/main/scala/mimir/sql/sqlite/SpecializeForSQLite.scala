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

      case _ => e

    }).recur( apply(_:Expression, schema) )
  }

  def apply(agg: AggFunction, schema: Map[String,Type]): AggFunction =
  {
    agg match {
      case AggFunction("FIRST", d, args, alias) =>
        Typechecker.typeOf(args(0), schema) match {
          case TInt()   => AggFunction("FIRST_INT", d, args, alias)
          case TFloat() => AggFunction("FIRST_FLOAT", d, args, alias)
          case t        => AggFunction("FIRST", d, args, alias)
        }
      case x => x
    }    
  }

  def apply(o: Operator): Operator = 
  {
    val schema = o.schema.toMap
    o.recurExpressions( 
      apply(_:Expression, schema) 
    ) match {
      case Aggregate(gb, agg, source) =>
        Aggregate(
          gb,
          agg.map( apply(_:AggFunction, schema) ),
          apply(source)
        )

      case o2 => 
        o2.recur( apply(_:Operator) )
    }
  }

}