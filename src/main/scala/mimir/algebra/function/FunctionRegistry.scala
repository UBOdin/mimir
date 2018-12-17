package mimir.algebra.function;

import java.sql.SQLException

import mimir.parser.ExpressionParser
import mimir.algebra._
import mimir.Database
import mimir.algebra.spark.function.SparkFunctions

sealed abstract class RegisteredFunction { val name: String }

case class NativeFunction(
	name: String, 
	evaluator: Seq[PrimitiveValue] => PrimitiveValue, 
	typechecker: Seq[BaseType] => BaseType,
	passthrough:Boolean = false
) extends RegisteredFunction

case class ExpressionFunction(
  name: String,
  args:Seq[String], 
  expr: Expression
) extends RegisteredFunction

case class FoldFunction(
  name: String, 
  expr: Expression
) extends RegisteredFunction

class FunctionRegistry {
	
	var functionPrototypes: scala.collection.mutable.Map[String, RegisteredFunction] = 
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
    fname:String,
    eval:Seq[PrimitiveValue] => PrimitiveValue, 
    typechecker: Seq[BaseType] => BaseType
  ): Unit =
    register(new NativeFunction(fname, eval, typechecker))
    
  def registerPassthrough(
    fname:String,
    eval:Seq[PrimitiveValue] => PrimitiveValue, 
    typechecker: Seq[BaseType] => BaseType
  ): Unit =
    register(new NativeFunction(fname, eval, typechecker, true))

  def registerExpr(fname:String, args:Seq[String], expr:String): Unit =
    registerExpr(fname, args, ExpressionParser.expr(expr))
  def registerExpr(fname:String, args:Seq[String], expr:Expression): Unit =
    register(new ExpressionFunction(fname, args, expr))

  def registerFold(fname:String, expr:String): Unit =
    registerFold(fname, ExpressionParser.expr(expr))
  def registerFold(fname:String, expr:Expression): Unit =
    register(new FoldFunction(fname, expr))

	def register(fn: RegisteredFunction) =
    functionPrototypes.put(fn.name, fn)

  def get(fname: String): RegisteredFunction =
  {
    functionPrototypes.get(fname) match { 
      case Some(func) => func
      case None => throw new RAException(s"Unknown function '$fname'")
    }
  }

  def getOption(fname: String): Option[RegisteredFunction] =
    functionPrototypes.get(fname)

  def unfold(fname: String, args: Seq[Expression]): Option[Expression] = 
    get(fname) match {
      case _:NativeFunction => None
      case ExpressionFunction(_, argNames, expr) => 
        Some(Eval.inline(expr, argNames.zip(args).toMap))
      case FoldFunction(_, expr) => 
        Some(
          args.tail.foldLeft[Expression](args.head){ case (curr,next) => 
            Eval.inline(expr, Map("CURR" -> curr, "NEXT" -> next)) }
        )

    }
}
