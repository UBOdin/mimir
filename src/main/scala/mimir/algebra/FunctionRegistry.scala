package mimir.algebra;

import java.sql.SQLException

import mimir.ctables._;

case class RegisteredFunction(fname: String, typechecker: List[Type] => Type)
{
	def getName = fname;
	def typecheck(args: List[Type]) = typechecker(args)
}

object FunctionRegistry {
	
	var functionPrototypes: scala.collection.mutable.Map[String, RegisteredFunction] = 
		scala.collection.mutable.Map.empty;

	{
		registerFunction("MIMIR_MAKE_ROWID", args => {
				if(!args.forall( t => (t == TRowId()) || (t == TAny()) )){
					throw new TypeException(TAny(), TRowId(), "MIMIR_MAKE_ROWID")
				}
				TRowId()
			})
		registerFunction(CTables.ROW_PROBABILITY, (_) => TString())
		registerFunction(CTables.VARIANCE, (_) => TFloat())
		registerFunction(CTables.CONFIDENCE, (_) => TFloat())
    registerFunction("__LIST_MIN", { (x: List[Type]) =>
    		Typechecker.assertNumeric(Typechecker.escalate(x)) 
    	})
    registerFunction("__LIST_MAX", { (x: List[Type]) =>
    		Typechecker.assertNumeric(Typechecker.escalate(x)) 
    	})
    registerFunction("CAST", (_) => TAny())
		registerFunction("MIMIRCAST", (_) => TAny())
		registerFunction("OTHERTEST", (_) => TInt())
		registerFunction("AGGTEST", (_) => TInt())
		registerFunction("DATE", _ match {
			case TString() :: List() => TDate()
			case _ => throw new SQLException("Invalid parameters to DATE()")
		})
		registerFunction("TO_DATE", _ match {
			case TString() :: TString() :: List() => TDate()
			case _ => throw new SQLException("Invalid parameters to DATE()")
		})
	}

	def registerFunction(fname: String, typechecker: List[Type] => Type): Unit =
	{
		functionPrototypes.put(fname, RegisteredFunction(fname, typechecker))
	}

	def typecheck(fname: String, args: List[Type]): Type =
	{
		functionPrototypes(fname).typecheck(args)
	}

}