package mimir.exec.sqlite

import java.sql._

import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.util._

object SpecializeForSQLite {

  def apply(e: Expression): Expression =
  {
    (e match {
      case Function(ID("cast"), Seq(target, TypePrimitive(t))) => 
        Function("mimircast", target, IntPrimitive(Type.id(t)))

      case Function(ID("cast"), _) =>
        throw new SQLException("Invalid CAST: "+e)

      case Function(ID("year_part"), Seq(d)) => 
        Function("cast", 
          Function("strftime", StringPrimitive("%Y"), d),
          TypePrimitive(TInt())
        )

      case Function(ID("month_part"), Seq(d)) => 
        Function("cast", 
          Function("strftime", StringPrimitive("%m"), d),
          TypePrimitive(TInt())
        )

      case Function(ID("day_part"), Seq(d)) => 
        Function("cast",
          Function("strftime", 
            StringPrimitive("%d"), d),
          TypePrimitive(TInt())
        )

      case Function(ID("json_extract_int"), args)   => Function("json_extract", args:_*)
      case Function(ID("json_extract_float"), args) => Function("json_extract", args:_*)
      case Function(ID("json_extract_str"), args)   => Function("json_extract", args:_*)

      case Function(ID("concat"), args) =>
        Function("printf", (Seq(
          StringPrimitive(args.map{ _ => "%s"}.mkString)
        ) ++ args):_*)
      case _ => e

    }).recur( apply(_:Expression) )
  }

  def apply(agg: AggFunction, typeOf: Expression => Type): AggFunction =
  {
    agg match {
      case AggFunction(ID("first"), d, args, alias) =>
        typeOf(args(0)) match {
          case TInt()   => AggFunction(ID("first_int"), d, args, alias)
          case TFloat() => AggFunction(ID("first_float"), d, args, alias)
          case t        => AggFunction(ID("first"), d, args, alias)
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