package mimir.algebra;

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
		registerFunction("JOIN_ROWIDS", _ match { 
				case TRowId :: TRowId :: List() => TRowId
				case _ => throw new TypeException(TAny, TRowId, "JOIN_ROWIDS")
			})
		registerFunction("__LEFT_UNION_ROWID", _ match { 
				case TRowId :: List() => TRowId
				case _ => throw new TypeException(TAny, TRowId, "UNION_ROWID")
			})
		registerFunction("__RIGHT_UNION_ROWID", _ match { 
				case TRowId :: List() => TRowId
				case _ => throw new TypeException(TAny, TRowId, "UNION_ROWID")
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