package mimir.sql.sqlite

import java.sql._

import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.util._

object SpecializeForSQLite {

  def apply(e: Expression): Expression =
  {
    (e match {
  
      case Function("CAST", Seq(target, TypePrimitive(t))) => 
        {//println("TYPE ID: "+t.id(t))
          Function("MIMIRCAST", Seq(target, IntPrimitive(Type.id(t))))}

      case Function("CAST", _) =>
        throw new SQLException("Invalid CAST: "+e)

      case Function("YEAR_PART", Seq(d)) => 
        Function("CAST", Seq(
          Function("STRFTIME", Seq(StringPrimitive("%Y"), d)),
          TypePrimitive(TInt())
        ))

      case Function("MONTH_PART", Seq(d)) => 
        Function("CAST", Seq(
          Function("STRFTIME", Seq(StringPrimitive("%m"), d)),
          TypePrimitive(TInt())
        ))

      case Function("DAY_PART", Seq(d)) => 
        Function("CAST", Seq(
          Function("STRFTIME", Seq(StringPrimitive("%d"), d)),
          TypePrimitive(TInt())
        ))

      case Function("JSON_EXTRACT_INT", args) => Function("JSON_EXTRACT", args)
      case Function("JSON_EXTRACT_FLOAT", args) => Function("JSON_EXTRACT", args)
      case Function("JSON_EXTRACT_STR", args) => Function("JSON_EXTRACT", args)

      case Function("CONCAT", args) =>
        Function("PRINTF", Seq(
          StringPrimitive(args.map{ _ => "%s"}.mkString)
        ) ++ args)

      case _ => e

    }).recur( apply(_:Expression) )
  }

  def apply(agg: AggFunction, typeOf: Expression => Type): AggFunction =
  {
    agg match {
      case AggFunction("FIRST", d, args, alias) =>
        typeOf(args(0)) match {
          case TInt()   => AggFunction("FIRST_INT", d, args, alias)
          case TFloat() => AggFunction("FIRST_FLOAT", d, args, alias)
          case t        => AggFunction("FIRST", d, args, alias)
        }
      case x => x
    }    
  }

  def apply(o: Operator, db: Database): Operator = 
  {
    o.recurExpressions( 
      apply(_:Expression) 
    ) match {
      case Aggregate(gb, agg, source) => {

        Aggregate(
          gb,
          agg.map( apply(_:AggFunction, db.typechecker.typeOf(_, source)) ),
          apply(source, db)
        )
      }

      case o2 => 
        o2.recur( apply(_:Operator, db) )
    }
  }

}