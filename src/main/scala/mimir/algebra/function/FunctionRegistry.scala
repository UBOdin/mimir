package mimir.algebra.function;

import java.sql.SQLException
import mimir.parser.ExpressionParser
import mimir.algebra._
import mimir.Database

sealed abstract class RegisteredFunction { val name: ID }

case class NativeFunction(
	name: ID, 
	evaluator: Seq[PrimitiveValue] => PrimitiveValue, 
	typechecker: Seq[Type] => Type,
	passthrough:Boolean = false
) extends RegisteredFunction

case class ExpressionFunction(
  name: ID,
  args:Seq[ID], 
  expr: Expression
) extends RegisteredFunction

case class FoldFunction(
  name: ID, 
  expr: Expression
) extends RegisteredFunction

class FunctionRegistry {
	
	var functionPrototypes: scala.collection.mutable.Map[ID, RegisteredFunction] = 
		scala.collection.mutable.Map.empty;

	{
    GeoFunctions.register(this)
    JsonFunctions.register(this)
    NumericFunctions.register(this)
    SampleFunctions.register(this)
    StringFunctions.register(this)
    TypeFunctions.register(this)
    UtilityFunctions.register(this)
    RandomnessFunctions.register(this)
    TimeFunctions.register(this)
    //it's to early to do this here.  Spark is not open yet. 
    //SparkFunctions.register(this)
	}

  def register(
    fname:ID,
    eval:Seq[PrimitiveValue] => PrimitiveValue, 
    typechecker: Seq[Type] => Type
  ): Unit =
    register(new NativeFunction(fname, eval, typechecker))
    
  def registerPassthrough(
    fname:ID,
    eval:Seq[PrimitiveValue] => PrimitiveValue, 
    typechecker: Seq[Type] => Type
  ): Unit =
    register(new NativeFunction(fname, eval, typechecker, true))

  def registerExpr(fname:ID, args:Seq[ID], expr:String): Unit =
    registerExpr(fname, args, ExpressionParser.expr(expr))
  def registerExpr(fname:ID, args:Seq[ID], expr:Expression): Unit =
    register(new ExpressionFunction(fname, args, expr))

  def registerFold(fname:ID, expr:String): Unit =
    registerFold(fname, ExpressionParser.expr(expr))
  def registerFold(fname:ID, expr:Expression): Unit =
    register(new FoldFunction(fname, expr))

	def register(fn: RegisteredFunction) =
    functionPrototypes.put(fn.name, fn)

  def get(fname: ID): RegisteredFunction =
  {
    functionPrototypes.get(fname) match { 
      case Some(func) => func
      case None => throw new RAException(s"Unknown function '$fname'")
    }
  }

  def getOption(fname: ID): Option[RegisteredFunction] =
    functionPrototypes.get(fname)

  def unfold(fname: ID, args: Seq[Expression]): Option[Expression] = 
    get(fname) match {
      case _:NativeFunction => None
      case ExpressionFunction(_, argNames, expr) => 
        Some(Eval.inline(expr, argNames.zip(args).toMap))
      case FoldFunction(_, expr) => 
        Some(
          args.tail.foldLeft[Expression](args.head){ case (curr,next) => 
            Eval.inline(expr, Map(ID("CURR") -> curr, ID("NEXT") -> next)) }
        )

    }
}
