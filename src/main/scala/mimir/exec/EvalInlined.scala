package mimir.exec;

import mimir.algebra._
import scala.util.matching._

class EvalInlined[T](scope: Map[String, (Type, (T => PrimitiveValue))])
{
  val typechecker = new ExpressionChecker(scope.mapValues(_._1))

  type Compiled[R] = (T => R)

  def typeOf(e: Expression): Type = typechecker.typeOf(e)

  def compile(e: Expression): Compiled[PrimitiveValue] = 
    compile(e, typeOf(e))

  def checkNull(op: Compiled[PrimitiveValue]): Compiled[PrimitiveValue] =
  {
    (t: T) => {
      try { op(t) }
      catch {
        case _:NullPointerException => NullPrimitive()
      }
    }
  }

  def compile(e: Expression, t:Type): Compiled[PrimitiveValue] =
  {
    e match { 
      case p:PrimitiveValue => { (_:T) => p }
      case Var(name) => scope(name)._2
      case _ => 
        t match {
          case TAny()       => 
            throw new RAException(s"Can not compile untyped expression: $e")
          case TInt()       => val v = compileForLong(e);   checkNull { (t:T) => IntPrimitive(v(t))    }
          case TFloat()     => val v = compileForDouble(e); checkNull { (t:T) => FloatPrimitive(v(t))  }
          case TBool()      => val v = compileForBool(e);   checkNull { (t:T) => BoolPrimitive(v(t))   }
          case TString()    => val v = compileForString(e); checkNull { (t:T) => StringPrimitive(v(t)) }
          case TType()      => val v = compileForType(e);   checkNull { (t:T) => TypePrimitive(v(t))   }
          case TDate()      => checkNull { compileForDate(e) }
          case TTimeStamp() => checkNull { compileForTimestamp(e) }
          case TRowId()     => checkNull { compileForRowId(e) }
          case TUser(ut)    => checkNull { compile(e, TypeRegistry.baseType(ut)) }
        }
    }
  }

  def compileProc(p: Proc): Compiled[PrimitiveValue] =
  {
    val args = p.getArgs.map { compile(_) }
    (t) => { p.get(args.map { _(t) }) }
  }

  def compileFunction(func: RegisteredFunction, argExprs: Seq[Expression]): Compiled[PrimitiveValue] =
  {
    val args = argExprs.map { compile(_) }
    (t) => { func.eval(args.map { _(t) }) }
  }
  def compileConditional[R](c: Expression, t: Expression, e: Expression, rcr: Expression => Compiled[R]): Compiled[R]=
  {
    val cv = compileForBool(c)
    val tv = rcr(t)
    val ev = rcr(e)
    (t) => { if(cv(t)){ tv(t) } else { ev(t) } }
  }
  def getVar(v: String): Compiled[PrimitiveValue] =
  {
    val l = scope(v)._2
    (t) => { 
      l(t) match { 
        case NullPrimitive() => throw new NullPointerException()
        case x => x
      }
    }
  }

  final def compileBinary[RA,RB](a: Expression, b: Expression, rcr: Expression => (T=>RA))(op:(RA,RA)=>RB): T => RB =
  {
    val la = rcr(a)
    val lb = rcr(b)
    (t) => { op(la(t), lb(t)) }
  }


  def compileForLong(e: Expression): Compiled[Long] = 
  {
    e match {
      case NullPrimitive()              => throw new NullPointerException()
      case pv:PrimitiveValue            => (t:T) => pv.asLong
      case Var(vname)                   => val l = getVar(vname); { l(_).asLong }
      case p:Proc                       => val l = compileProc(p); { l(_).asLong }
      case Arithmetic(op, lhs, rhs)     => {
        val lv = compileForLong(lhs)
        val rv = compileForLong(rhs)
        op match {
          case Arith.Add        => (t:T) => { lv(t) + rv(t) }
          case Arith.Mult       => (t:T) => { lv(t) * rv(t) }
          case Arith.Sub        => (t:T) => { lv(t) - rv(t) }
          case Arith.Div        => (t:T) => { lv(t) / rv(t) }
          case Arith.BitAnd     => (t:T) => { lv(t) & rv(t) }
          case Arith.BitOr      => (t:T) => { lv(t) | rv(t) }
          case Arith.ShiftLeft  => (t:T) => { lv(t) << rv(t) }
          case Arith.ShiftRight => (t:T) => { lv(t) >> rv(t) }
        }
      }
      case Function(name, args) => {
        val func = FunctionRegistry.functionPrototypes(name)
        func.unfold(args) match {
          case Some(fExpr) => compileForLong(fExpr)
          case None => val l = compileFunction(func, args); { l(_).asLong }
        }
      }
      case Conditional(c, t, e) => compileConditional(c, t, e, compileForLong)
      case _ => throw new RAException(s"Invalid Expression on Int: $e")
    }
  }
  def compileForDouble(e: Expression): Compiled[Double] = 
  {
    e match {
      case NullPrimitive()              => throw new NullPointerException()
      case pv:PrimitiveValue            => (t:T) => pv.asDouble
      case Var(vname)                   => val l = getVar(vname); { l(_).asDouble }
      case p:Proc                       => val l = compileProc(p); { l(_).asDouble }
      case Arithmetic(op, lhs, rhs)     => {
        val lv = compileForDouble(lhs)
        val rv = compileForDouble(rhs)
        op match {
          case Arith.Add        => (t:T) => { lv(t) + rv(t) }
          case Arith.Mult       => (t:T) => { lv(t) * rv(t) }
          case Arith.Sub        => (t:T) => { lv(t) - rv(t) }
          case Arith.Div        => (t:T) => { lv(t) / rv(t) }
          case _                => throw new RAException(s"Invalid Arithmetic on Float: $op")
        }
      }
      case Function(name, args) => {
        val func = FunctionRegistry.functionPrototypes(name)
        func.unfold(args) match {
          case Some(fExpr) => compileForDouble(fExpr)
          case None => val l = compileFunction(func, args); { l(_).asDouble }
        }
      }
      case Conditional(c, t, e) => compileConditional(c, t, e, compileForDouble)
      case _ => throw new RAException(s"Invalid Expression on Float: $e")
    }
  }

  final def likeToRE(s: String): Regex = 
    s.replaceAll("[.()\\[\\]]", "\\\\.").replaceAll("%", ".*").replaceAll("\\?", ".").r


  def compileForBool(e: Expression): Compiled[Boolean] = 
  {
    e match {
      case NullPrimitive()              => throw new NullPointerException()
      case pv:PrimitiveValue            => (t:T) => pv.asBool
      case Var(vname)                   => val l = getVar(vname); { l(_).asBool }
      case p:Proc                       => val l = compileProc(p); { l(_).asBool }
      case Arithmetic(op, lhs, rhs)     => {
        val lv = compileForBool(lhs)
        val rv = compileForBool(rhs)
        op match {
          case Arith.And  => (t:T) => { lv(t) && rv(t) }
          case Arith.Or   => (t:T) => { lv(t) || rv(t) }
          case _                => throw new RAException(s"Invalid Arithmetic on Float: $op")
        }
      }
      case Comparison(op, lhs, rhs)     => {
        (op, Type.rootType(typeOf(lhs)), Type.rootType(typeOf(rhs))) match {
          case (_, TAny(), _) => throw new RAException(s"Invalid comparison on TAny: $e")
          case (_, _, TAny()) => throw new RAException(s"Invalid comparison on TAny: $e")
          case (_, TUser(n), _) => throw new RAException(s"Internal error in Type.rootType($n): $e")
          case (_, _, TUser(n)) => throw new RAException(s"Internal error in Type.rootType($n): $e")
          case (Cmp.Eq, TBool(), TBool())           => compileBinary(lhs, rhs, compileForBool) { _ == _ }
          case (Cmp.Eq, TInt(), TInt())             => compileBinary(lhs, rhs, compileForLong) { _ == _ }
          case (Cmp.Eq, ( TInt() | TFloat() ), ( TInt() | TFloat() ) ) 
                                                    => compileBinary(lhs, rhs, compileForDouble) { _ == _ }
          case (Cmp.Eq, TString(), TString())       => compileBinary(lhs, rhs, compileForString) { _.equals(_) }
          case (Cmp.Eq, TType(), TType())           => compileBinary(lhs, rhs, compileForType) { _.equals(_) }
          case (Cmp.Eq, TDate(), TDate())           => compileBinary(lhs, rhs, compileForDate) { _.equals(_) }
          case (Cmp.Eq, TTimeStamp(), TTimeStamp()) => compileBinary(lhs, rhs, compileForTimestamp) { _.equals(_) }
          case (Cmp.Eq, TRowId(), TRowId())         => compileBinary(lhs, rhs, compileForRowId) { _.equals(_) }
          case (Cmp.Eq, _, _) 
              => throw new RAException(s"Invalid comparison: $e")
          case (Cmp.Neq, _, _)                      => compileForBool(ExpressionUtils.makeNot(Comparison(Cmp.Eq, lhs, rhs)))
          case (Cmp.Gt, TInt(), TInt())             => compileBinary(lhs, rhs, compileForLong) { _ > _ }
          case (Cmp.Gte, TInt(), TInt())            => compileBinary(lhs, rhs, compileForLong) { _ >= _ }
          case (Cmp.Lt, TInt(), TInt())             => compileBinary(lhs, rhs, compileForLong) { _ < _ }
          case (Cmp.Lte, TInt(), TInt())            => compileBinary(lhs, rhs, compileForLong) { _ <= _ }
          case (Cmp.Gt, ( TInt() | TFloat() ), ( TInt() | TFloat() ))
                                                    => compileBinary(lhs, rhs, compileForDouble) { _ > _ }
          case (Cmp.Gte, ( TInt() | TFloat() ), ( TInt() | TFloat() ))
                                                    => compileBinary(lhs, rhs, compileForDouble) { _ >= _ }
          case (Cmp.Lt, ( TInt() | TFloat() ), ( TInt() | TFloat() ))
                                                    => compileBinary(lhs, rhs, compileForDouble) { _ < _ }
          case (Cmp.Lte, ( TInt() | TFloat() ), ( TInt() | TFloat() ))
                                                    => compileBinary(lhs, rhs, compileForDouble) { _ <= _ }
          case (Cmp.Gt, TDate(), TDate())           => compileBinary(lhs, rhs, compileForDate) { _ > _ }
          case (Cmp.Gte, TDate(), TDate())          => compileBinary(lhs, rhs, compileForDate) { _ >= _ }
          case (Cmp.Lt, TDate(), TDate())           => compileBinary(lhs, rhs, compileForDate) { _ < _ }
          case (Cmp.Lte, TDate(), TDate())          => compileBinary(lhs, rhs, compileForDate) { _ <= _ }
          case (Cmp.Gt, TTimeStamp(), TTimeStamp()) => compileBinary(lhs, rhs, compileForTimestamp) { _ > _ }
          case (Cmp.Gte, TTimeStamp(), TTimeStamp())=> compileBinary(lhs, rhs, compileForTimestamp) { _ >= _ }
          case (Cmp.Lt, TTimeStamp(), TTimeStamp()) => compileBinary(lhs, rhs, compileForTimestamp) { _ < _ }
          case (Cmp.Lte, TTimeStamp(), TTimeStamp())=> compileBinary(lhs, rhs, compileForTimestamp) { _ <= _ }
          case (Cmp.Like, TString(), TString()) => {
            val lv = compileForString(rhs)
            rhs match {
              case StringPrimitive(s) => 
                val re = likeToRE(s)
                (t) => { re.findFirstIn(lv(t)) != None }
              case _ => 
                val rv = compileForString(rhs)
                (t) => { likeToRE(rv(t)).findFirstIn(lv(t)) != None }
            }
          }
          case (Cmp.Like, _, _) => throw new RAException(s"Invalid comparison: LIKE: $e")
          case (Cmp.NotLike, TString(), TString()) => compileForBool(ExpressionUtils.makeNot(Comparison(Cmp.Like, lhs, rhs)))
          case (Cmp.NotLike, _, _) => throw new RAException(s"Invalid comparison: NOT LIKE: $e")
          case (_, a, b) => throw new RAException(s"Invalid comparison: $a $op $b")

        }
      }
      case Not(e)                       => val l = compileForBool(e); { !l(_) }
      case IsNullExpression(e)          => compileForIsNull(e) match { case Left(x) => {(t) => x}; case Right(x) => x}
      case Function(name, args) => {
        val func = FunctionRegistry.functionPrototypes(name)
        func.unfold(args) match {
          case Some(fExpr) => compileForBool(fExpr)
          case None => val l = compileFunction(func, args); { l(_).asBool }
        }
      }
      case Conditional(c, t, e) => compileConditional(c, t, e, compileForBool)
      case _ => throw new RAException(s"Invalid Expression on Bool: $e")
    }
  }

  final def compilePassthrough[R](e: Expression, rcr: Expression => Compiled[R], prim: PrimitiveValue => R): Compiled[R] =
  {
    e match {
      case NullPrimitive()              => throw new NullPointerException()
      case pv:PrimitiveValue            => (t:T) => prim(pv)
      case Var(vname)                   => val l = getVar(vname); (t) => prim(l(t))
      case p:Proc                       => val l = compileProc(p); (t) => prim(l(t))
      case Function(name, args) => {
        val func = FunctionRegistry.functionPrototypes(name)
        func.unfold(args) match {
          case Some(fExpr) => rcr(fExpr)
          case None => val l = compileFunction(func, args); (t) => prim(l(t))
        }
      }
      case Conditional(c, t, e) => compileConditional(c, t, e, rcr)
      case _ => throw new RAException(s"Invalid Passthrough Expression: $e")
    }
  }

  def compileForString(e: Expression): Compiled[String] = 
    compilePassthrough(e, compileForString, _.asString)
  def compileForType(e: Expression): Compiled[Type] = 
    compilePassthrough(e, compileForType, _.asInstanceOf[TypePrimitive].t)
  def compileForDate(e: Expression): Compiled[DatePrimitive] = 
    compilePassthrough(e, compileForDate, _.asInstanceOf[DatePrimitive])    
  def compileForTimestamp(e: Expression): Compiled[TimestampPrimitive] = 
    compilePassthrough(e, compileForTimestamp, _.asInstanceOf[TimestampPrimitive])    
  def compileForRowId(e: Expression): Compiled[RowIdPrimitive] = 
    compilePassthrough(e, compileForRowId, _.asInstanceOf[RowIdPrimitive])    
  def and(a: Compiled[Boolean], b: Compiled[Boolean]): Compiled[Boolean] =
    { (t) => a(t) && b(t) }
  def or(a: Compiled[Boolean], b: Compiled[Boolean]): Compiled[Boolean] =
    { (t) => a(t) || b(t) }
  def compileForIsNull(e: Expression): Either[Boolean, Compiled[Boolean]] = 
  {
    e match {
      case _:NullPrimitive => Left(true)
      case _:PrimitiveValue => Left(false)
      case Var(vname) => {
        val v = scope(vname)._2
        Right( { v(_) match { case NullPrimitive() => true; case _ => false } } )
      }
      case Arithmetic(Arith.And, lhs, rhs) => {
        (compileForIsNull(lhs), compileForIsNull(rhs)) match {
          case (Left(true), Left(true)) => Left(true)
          case (Left(false), Left(false)) => Left(false)
          case (Left(true), Left(false)) => Right(compileForBool(ExpressionUtils.makeNot(rhs)))
          case (Left(false), Left(true)) => Right(compileForBool(ExpressionUtils.makeNot(lhs)))
          case (Left(true), Right(rv)) => Right(or(rv, compileForBool(rhs)))
          case (Right(lv), Left(true)) => Right(or(lv, compileForBool(lhs)))
          case (Left(false), Right(rv)) => Right(and(rv, compileForBool(rhs)))
          case (Right(lv), Left(false)) => Right(and(lv, compileForBool(lhs)))
          case (Right(lv), Right(rv)) => 
            Right(or(or(
              and(lv, rv),
              and(lv, compileForBool(rhs))
            ), and(rv, compileForBool(lhs))))
        }
      }
      case Arithmetic(Arith.Or, lhs, rhs) => {
        (compileForIsNull(lhs), compileForIsNull(rhs)) match {
          case (Left(true), Left(true)) => Left(true)
          case (Left(false), Left(false)) => Left(false)
          case (Left(true), Left(false)) => Right(compileForBool(rhs))
          case (Left(false), Left(true)) => Right(compileForBool(lhs))
          case (Left(true), Right(rv)) => Right(or(rv, compileForBool(ExpressionUtils.makeNot(rhs))))
          case (Right(lv), Left(true)) => Right(or(lv, compileForBool(ExpressionUtils.makeNot(lhs))))
          case (Left(false), Right(rv)) => Right(and(rv, compileForBool(ExpressionUtils.makeNot(rhs))))
          case (Right(lv), Left(false)) => Right(and(lv, compileForBool(ExpressionUtils.makeNot(lhs))))
          case (Right(lv), Right(rv)) => 
            Right(or(or(
              and(lv, rv),
              and(lv, compileForBool(ExpressionUtils.makeNot(rhs)))
            ), and(rv, compileForBool(ExpressionUtils.makeNot(lhs)))))
        }
      }
      case Arithmetic(_, lhs, rhs) => {
        (compileForIsNull(lhs), compileForIsNull(rhs)) match {
          case (Left(true), _) => Left(true)
          case (_, Left(true)) => Left(true)
          case (l, Left(false)) => l
          case (Left(false), r) => r
          case (Right(l), Right(r)) => Right( (t) => { l(t) && r(t) } )
        }
      }
      case Comparison(_, lhs, rhs) => {
        (compileForIsNull(lhs), compileForIsNull(rhs)) match {
          case (Left(true), _) => Left(true)
          case (_, Left(true)) => Left(true)
          case (l, Left(false)) => l
          case (Left(false), r) => r
          case (Right(l), Right(r)) => Right( {(t) => { l(t) && r(t) } } )
        }
      }
      case Not(ne) => compileForIsNull(ne)
      case Conditional(c, t, e) => {
        compileForIsNull(
          Arithmetic(Arith.Or, 
            Arithmetic(Arith.And, c, t),
            Arithmetic(Arith.And, ExpressionUtils.makeNot(c), e)
          )
        )
      }
    }
  }

}