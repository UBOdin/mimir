package mimir.algebra;

import java.sql.SQLException

import mimir.parser.SimpleExpressionParser
import mimir.algebra.function._

class RegisteredFunction(
	val name: String, 
	evaluator: Seq[PrimitiveValue] => PrimitiveValue, 
	typechecker: Seq[Type] => Type
) {
	def typecheck(args: Seq[Type]) = typechecker(args)
	def eval(args: Seq[PrimitiveValue]) = evaluator(args)
  def unfold(args: Seq[Expression]): Option[Expression] = None
}

class ExpressionFunction(name: String, val args:Seq[String], val expr: Expression)
  extends RegisteredFunction(name, 
    (argVal) => Eval.eval(expr, args.zip(argVal).toMap),
    (argT) => Typechecker.typeOf(expr, args.zip(argT).toMap)
  )
{
  override def unfold(argExprs: Seq[Expression]) = 
    Some(Eval.inline(expr, args.zip(argExprs).toMap))
}

class FoldFunction(name: String, expr: Expression)
  extends RegisteredFunction(name, 
    (args) => args.tail.foldLeft(args.head){ case (curr,next) => 
                Eval.eval(expr, Map("CURR" -> curr, "NEXT" -> next)) },
    (args) => args.tail.foldLeft(args.head){ case (curr,next) => 
                Typechecker.typeOf(expr, Map("CURR" -> curr, "NEXT" -> next)) }
  )
{
  override def unfold(args: Seq[Expression]) = 
    Some(
      args.tail.foldLeft[Expression](args.head){ case (curr,next) => 
        Eval.inline(expr, Map("CURR" -> curr, "NEXT" -> next)) }
    )
}

object FunctionRegistry {
	
	var functionPrototypes: scala.collection.mutable.Map[String, RegisteredFunction] = 
		scala.collection.mutable.Map.empty;

	{
    GeoFunctions.register()
    JsonFunctions.register()
    NumericFunctions.register()
    SampleFunctions.register()
    StringFunctions.register()
    TypeFunctions.register()
    UtilityFunctions.register()
    RandomnessFunctions.register()
	}

	def registerSet(
		fnames: Seq[String], 
		eval:Seq[PrimitiveValue] => PrimitiveValue, 
		typechecker: Seq[Type] => Type
	): Unit =
		fnames.map(registerNative(_, eval, typechecker))

  def registerNative(
    fname:String,
    eval:Seq[PrimitiveValue] => PrimitiveValue, 
    typechecker: Seq[Type] => Type
  ): Unit =
    register(new RegisteredFunction(fname, eval, typechecker))

  def registerExpr(fname:String, args:Seq[String], expr:String): Unit =
    registerExpr(fname, args, SimpleExpressionParser.expr(expr))
  def registerExpr(fname:String, args:Seq[String], expr:Expression): Unit =
    register(new ExpressionFunction(fname, args, expr))

  def registerFold(fname:String, expr:String): Unit =
    registerFold(fname, SimpleExpressionParser.expr(expr))
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

	def typecheck(fname: String, args: Seq[Type]): Type = {
    try {
  		get(fname).typecheck(args)
    } catch {
      case TypeException(found, expected, detail, None) =>
        throw TypeException(found, expected, detail, Some(Function(fname, args.map{ TypePrimitive(_) })))
    }
  }

	def eval(fname: String, args: Seq[PrimitiveValue]): PrimitiveValue =
		functionPrototypes(fname).eval(args)

  def unfold(fname: String, args: Seq[Expression]): Option[Expression] = 
    functionPrototypes(fname).unfold(args)
}
