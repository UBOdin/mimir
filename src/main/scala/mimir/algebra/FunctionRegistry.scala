package mimir.algebra;

import java.sql.SQLException

import mimir.provenance._
import mimir.ctables._
import mimir.util._

case class RegisteredFunction(
	fname: String, 
	evaluator: Seq[PrimitiveValue] => PrimitiveValue, 
	typechecker: Seq[Type] => Type
) {
	def getName = fname;
	def typecheck(args: Seq[Type]) = typechecker(args)
	def eval(args: Seq[PrimitiveValue]) = evaluator(args)
}

object FunctionRegistry {
	
	var functionPrototypes: scala.collection.mutable.Map[String, RegisteredFunction] = 
		scala.collection.mutable.Map.empty;

	{
		register("MIMIR_MAKE_ROWID", 
      Provenance.joinRowIds(_: Seq[PrimitiveValue]),
			((args: Seq[Type]) => 
				if(!args.forall( t => (t == TRowId()) || (t == TAny()) )) { 
					throw new TypeException(TAny(), TRowId(), "MIMIR_MAKE_ROWID")
				} else {
					TRowId()
				}
			)
		)

    register("__Seq_MIN", 
    	(params: Seq[PrimitiveValue]) => {
        FloatPrimitive(params.map( x => 
          try { x.asDouble } 
          catch { case e:TypeException => Double.MaxValue }
        ).min)
      },
    	{ (x: Seq[Type]) => 
    		Typechecker.assertNumeric(Typechecker.escalate(x), Function("__LIST_MIN", List())) 
    	}
    )

    register("__LIST_MAX", 
    	(params: Seq[PrimitiveValue]) => {
        FloatPrimitive(params.map( x => 
          try { x.asDouble } 
          catch { case e:TypeException => Double.MinValue }
        ).max)
      },
      { (x: Seq[Type]) => 
    		Typechecker.assertNumeric(Typechecker.escalate(x), Function("__LIST_MAX", List())) 
    	}
    )

    registerSet(List("CAST", "MIMIRCAST"), 
      (params: Seq[PrimitiveValue]) => {
        params match {
          case x :: TypePrimitive(t)    :: Nil => Cast(t, x)
          case _ => throw new SQLException("Invalid cast: "+params)
        }
      },
      (_) => TAny()
    )

		registerSet(List("DATE", "TO_DATE"), 
		  (params: Seq[PrimitiveValue]) => 
          { TextUtils.parseDate(params.head.asString) },
		  _ match {
		    case TString() :: Nil => TDate()
		    case _ => throw new SQLException("Invalid parameters to DATE()")
		  }
		)

		register("ABSOLUTE", 
			{
	      case Seq(IntPrimitive(i))   => if(i < 0){ IntPrimitive(-i) } else { IntPrimitive(i) }
	      case Seq(FloatPrimitive(f)) => if(f < 0){ FloatPrimitive(-f) } else { FloatPrimitive(f) }
	      case Seq(NullPrimitive())   => NullPrimitive()
	      case x => throw new SQLException("Non-numeric parameter to absolute: '"+x+"'")
	    },
			(x: Seq[Type]) => Typechecker.assertNumeric(x(0), Function("ABSOLUTE", List()))
		)

    register("SQRT",
      {
        case Seq(n:NumericPrimitive) => FloatPrimitive(Math.sqrt(n.asDouble))
      },
      (x: Seq[Type]) => Typechecker.assertNumeric(x(0), Function("ABSOLUTE", List()))
    )

    register("BITWISE_AND", (x) => IntPrimitive(x(0).asLong & x(1).asLong), (_) => TInt())

    register("JSON_EXTRACT",(_) => ???, (_) => TAny())
    register("JSON_ARRAY_LENGTH",(_) => ???, (_) => TInt())
	}

	def registerSet(
		fnames: Seq[String], 
		eval:Seq[PrimitiveValue] => PrimitiveValue, 
		typechecker: Seq[Type] => Type
	): Unit =
		fnames.map(register(_, eval, typechecker))

	def register(
		fname: String, 
		eval: Seq[PrimitiveValue] => PrimitiveValue, 
		typechecker: Seq[Type] => Type
	): Unit =
		functionPrototypes.put(fname, RegisteredFunction(fname, eval, typechecker))

	def typecheck(fname: String, args: Seq[Type]): Type = 
		functionPrototypes(fname).typecheck(args)

	def eval(fname: String, args: Seq[PrimitiveValue]): PrimitiveValue =
		functionPrototypes(fname).eval(args)

}