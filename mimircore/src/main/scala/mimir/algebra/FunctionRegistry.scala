package mimir.algebra;

import java.sql.SQLException

import mimir.algebra.Type._;
import mimir.ctables._;

case class RegisteredFunction(fname: String, typechecker: List[Type.T] => Type.T)
{
	def getName = fname;
	def typecheck(args: List[Type.T]) = typechecker(args)
}

object FunctionRegistry {
	
	var functionPrototypes: scala.collection.mutable.Map[String, RegisteredFunction] = 
		scala.collection.mutable.Map.empty;

	{
		registerFunction("MIMIR_MAKE_ROWID", args => {
				if(!args.forall( t => (t == TRowId) || (t == TAny) )){ 
					throw new TypeException(TAny, TRowId, "MIMIR_MAKE_ROWID")
				}
				TRowId
			})
		registerFunction(CTables.ROW_PROBABILITY, (_) => TString)
		registerFunction(CTables.VARIANCE, (_) => TFloat)
		registerFunction(CTables.CONFIDENCE, (_) => TFloat)
    registerFunction("__LIST_MIN", { (x: List[Type.T]) => 
    		Typechecker.assertNumeric(Typechecker.escalate(x)) 
    	})
    registerFunction("__LIST_MAX", { (x: List[Type.T]) => 
    		Typechecker.assertNumeric(Typechecker.escalate(x)) 
    	})
    registerFunction("CAST", (_) => TAny)
		registerFunction("MIMIRCAST", (_) => TAny)
		registerFunction("OTHERTEST", (_) => TInt)
		registerFunction("AGGTEST", (_) => TInt)
    registerFunction("BOOLAND", (_) => TBool)
		registerFunction("DATE", _ match {
			case TString :: List() => TDate
			case _ => throw new SQLException("Invalid parameters to DATE()")
		})
		registerFunction("TO_DATE", _ match {
			case TString :: TString :: List() => TDate
			case _ => throw new SQLException("Invalid parameters to DATE()")
		})
	}

	def registerFunction(fname: String, typechecker: List[Type.T] => Type.T): Unit =
	{
		functionPrototypes.put(fname, RegisteredFunction(fname, typechecker))
	}

	def typecheck(fname: String, args: List[Type.T]): Type.T = 
	{
		functionPrototypes(fname).typecheck(args)
	}

}