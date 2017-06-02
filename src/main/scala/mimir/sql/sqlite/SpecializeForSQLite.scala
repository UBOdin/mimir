package mimir.sql.sqlite;

import java.sql._;

import mimir.algebra._;
import mimir.ctables._;
import mimir.util._;

object SpecializeForSQLite {

  def apply(e: Expression, schema: Map[String, Type]): Expression =
  {
    (e match {
  
      case Function("CAST", Seq(target, TypePrimitive(t))) => 
        {//println("TYPE ID: "+t.id(t))
          Function("MIMIRCAST", Seq(target, IntPrimitive(Type.id(t))))}

      case Function("CAST", _) =>
        throw new SQLException("Invalid CAST: "+e)

      case Function("JSON_EXTRACT_INT", args) => Function("JSON_EXTRACT", args)
      case Function("JSON_EXTRACT_FLOAT", args) => Function("JSON_EXTRACT", args)
      case Function("JSON_EXTRACT_STR", args) => Function("JSON_EXTRACT", args)

      case Function("CONCAT", args) =>
        Function("PRINTF", Seq(
          StringPrimitive(args.map{ _ => "%s"}.mkString)
        ) ++ args)

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