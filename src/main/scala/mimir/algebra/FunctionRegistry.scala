package mimir.algebra;

import java.sql.SQLException

import mimir.provenance._
import mimir.ctables._
import mimir.util._
import mimir.exec.{TupleBundler,WorldBits}
import mimir.parser.SimpleExpressionParser
import org.geotools.referencing.datum.DefaultEllipsoid
import org.joda.time.DateTime

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
	  registerNative("POSSION", 
      {
	      case Seq(IntPrimitive(m))   => {
          IntPrimitive(mimir.sql.sqlite.Possion.poisson_helper(m))
	      }
        case Seq(FloatPrimitive(m))   => {
          IntPrimitive(mimir.sql.sqlite.Possion.poisson_helper(m))
	      }
        case Seq(NullPrimitive())   => NullPrimitive()
	      case x => throw new SQLException("Non-numeric parameter to possion: '"+x+"'")
      },
      ((args: Seq[Type]) => TInt())
		)
		
		registerNative("GAMMA", 
      {
	      case Seq(FloatPrimitive(k), FloatPrimitive(theta))   => {
          FloatPrimitive(mimir.sql.sqlite.Gamma.sampleGamma(k, theta))
	      }
        case Seq(NullPrimitive(), FloatPrimitive(r))   => NullPrimitive()
        case Seq(FloatPrimitive(r), NullPrimitive())   => NullPrimitive()
        case Seq(NullPrimitive(), NullPrimitive())   => NullPrimitive()
	      case x => throw new SQLException("Non-numeric parameter to gamma: '"+x+"'")
      },
      ((args: Seq[Type]) => TFloat())
		)
	  
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
		    case _ :: Nil => TDate()
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

		registerNative("ROUND", 
			{
	      case Seq(IntPrimitive(i))   =>  IntPrimitive(i) 
	      case Seq(FloatPrimitive(f)) => IntPrimitive(Math.round(f) )
	      case Seq(NullPrimitive())   => NullPrimitive()
	      case x => throw new SQLException("Non-numeric parameter to round: '"+x+"'")
	    },
			(x: Seq[Type]) => Typechecker.assertNumeric(x(0), Function("ROUND", List()))
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

    registerNative(
      "DST",
      (args) => {
        FloatPrimitive(DefaultEllipsoid.WGS84.orthodromicDistance(
          args(0).asDouble, //lon1
          args(1).asDouble, //lat1
          args(2).asDouble, //lon2
          args(3).asDouble  //lat2
        ))
      },
      (args) => {
        (0 until 4).foreach { i => Typechecker.assertNumeric(args(i), Function("DST", List())) }; 
        TFloat()
      }
    )
    registerNative(
      "SPEED",
      (args) => {
        val distance: Double = args(0).asDouble
        val startingDate: DateTime = args(1).asDateTime
        val endingDate: DateTime =
          args(2) match {
            case NullPrimitive() => new DateTime()
            case x => x.asDateTime
          }

        val numberOfHours: Long = Math.abs(endingDate.getMillis - startingDate.getMillis) / 1000 / 60 / 60

        FloatPrimitive(distance / 1000 / numberOfHours) // kmph
      },
      (_) => TFloat()
    )
    registerNative(
      "JULIANDAY",
      (args) => ???,
      (_) => TInt()
    )

    registerNative("JSON_EXTRACT",(_) => ???, (_) => TAny())
    registerNative("JSON_ARRAY",(_) => ???, (_) => TString())
    registerNative("JSON_ARRAY_LENGTH",(_) => ???, (_) => TInt())
    registerNative("JSON_OBJECT", (_) => ???, (_) => TString())

    registerNative("BEST_SAMPLE", 
      (args: Seq[PrimitiveValue]) => {
        TupleBundler.mostLikelyValue(
          args.head.asLong,
          args.tail.grouped(2).
            map { arg => (arg(1), arg(0).asDouble) }.toSeq
        ) match {
          case Some(v) => v
          case None => NullPrimitive()
        }
      },
      (types: Seq[Type]) => {
        Typechecker.assertNumeric(types.head, 
          Function("BEST_SAMPLE", types.map(TypePrimitive(_))))
        Typechecker.escalate(
          types.tail.grouped(2).
            map { t => 
              Typechecker.assertNumeric(t(0),
                Function("BEST_SAMPLE", types.map(TypePrimitive(_))))
              t(1)
            }
        )
      }
    )

    registerNative("YEAR_PART", 
      (args: Seq[PrimitiveValue]) => IntPrimitive(args(0).asDateTime.getYear),
      (types: Seq[Type]) => TInt()
    )

    registerNative("MONTH_PART", 
      (args: Seq[PrimitiveValue]) => IntPrimitive(args(0).asDateTime.getMonthOfYear),
      (types: Seq[Type]) => TInt()
    )

    registerNative("DAY_PART", 
      (args: Seq[PrimitiveValue]) => IntPrimitive(args(0).asDateTime.getDayOfMonth),
      (types: Seq[Type]) => TInt()
    )

    registerNative("SAMPLE_CONFIDENCE",
      (args: Seq[PrimitiveValue]) => 
        FloatPrimitive(
          WorldBits.confidence(args(0).asLong, args(0).asLong.toInt)
        ),
      (types: Seq[Type]) => {
        Typechecker.assertNumeric(types(0), 
          Function("SAMPLE_CONFIDENCE", types.map(TypePrimitive(_))))
        Typechecker.assertNumeric(types(1),
          Function("SAMPLE_CONFIDENCE", types.map(TypePrimitive(_))))
        TFloat()
      }
    )

    val prng = new scala.util.Random()
    registerNative("RANDOM",
      (args: Seq[PrimitiveValue]) => IntPrimitive(prng.nextLong),
      (types: Seq[Type]) => { TInt() }
    )

    registerNative("AVG",(_) => ???, (_) => TInt())
    registerNative("STRFTIME",(_) => ???, (_) => TInt())
    registerNative("STDDEV",(_) => ???, (_) => TFloat())
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
