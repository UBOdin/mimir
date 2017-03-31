package mimir.algebra;

import java.sql.SQLException

import mimir.provenance._
import mimir.ctables._
import mimir.util._
import mimir.parser.SimpleExpressionParser

class RegisteredFunction(
	val name: String, 
	evaluator: Seq[PrimitiveValue] => PrimitiveValue, 
	typechecker: Seq[Type] => Type
) {
	def typecheck(args: Seq[Type]) = typechecker(args)
	def eval(args: Seq[PrimitiveValue]) = evaluator(args)
  def unfold(args: Seq[Expression]): Option[Expression] = None
}

class ExpressionFunction(name: String, args:Seq[String], expr: Expression)
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
		registerNative("MIMIR_MAKE_ROWID", 
      Provenance.joinRowIds(_: Seq[PrimitiveValue]),
			((args: Seq[Type]) => TRowId())
		)

    registerFold("SEQ_MIN", "IF CURR < NEXT THEN CURR ELSE NEXT END")
    registerFold("SEQ_MAX", "IF CURR > NEXT THEN CURR ELSE NEXT END")

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

		registerNative("ABSOLUTE", 
			{
	      case Seq(IntPrimitive(i))   => if(i < 0){ IntPrimitive(-i) } else { IntPrimitive(i) }
	      case Seq(FloatPrimitive(f)) => if(f < 0){ FloatPrimitive(-f) } else { FloatPrimitive(f) }
	      case Seq(NullPrimitive())   => NullPrimitive()
	      case x => throw new SQLException("Non-numeric parameter to absolute: '"+x+"'")
	    },
			(x: Seq[Type]) => Typechecker.assertNumeric(x(0), Function("ABSOLUTE", List()))
		)

    registerNative("SQRT",
      {
        case Seq(n:NumericPrimitive) => FloatPrimitive(Math.sqrt(n.asDouble))
      },
      (x: Seq[Type]) => Typechecker.assertNumeric(x(0), Function("SQRT", List()))
    )
    registerExpr("DISTANCE", List("A", "B"), 
      Function("SQRT", List(
        Arithmetic(Arith.Add,
          Arithmetic(Arith.Mult, Var("A"), Var("A")),
          Arithmetic(Arith.Mult, Var("B"), Var("B"))
      ))))

    registerNative("BITWISE_AND", (x) => IntPrimitive(x(0).asLong & x(1).asLong), (_) => TInt())

    FunctionRegistry.registerNative(
      "DST",
      (args) => { throw new SQLException("Mimir Cannot Execute VGTerm Functions Internally") },
      (_) => TFloat()
    )
    FunctionRegistry.registerNative(
      "SPEED",
      (args) => { throw new SQLException("Mimir Cannot Execute VGTerm Functions Internally") },
      (_) => TFloat()
    )
    FunctionRegistry.registerNative(
      "JULIANDAY",
      (args) => { throw new SQLException("Mimir Cannot Execute VGTerm Functions Internally") },
      (_) => TInt()
    )

    registerNative("JSON_EXTRACT",(_) => ???, (_) => TAny())
    registerNative("JSON_ARRAY",(_) => ???, (_) => TInt())
    registerNative("JSON_ARRAY_LENGTH",(_) => ???, (_) => TInt())
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

	def typecheck(fname: String, args: Seq[Type]): Type = {
    try {
  		functionPrototypes(fname).typecheck(args)
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