package mimir.algebra;

import mimir.ctables.CTables

import scala.reflect.runtime.universe._

import java.sql._;

import mimir.util._;

class TypeException(found: Type.T, expected: Type.T, 
                    context:String) 
  extends Exception(
    "Type Mismatch ["+context+
    "]: found "+found.toString+
    ", but expected "+expected.toString
  );
class RAException(msg: String) extends Exception(msg);

object Type extends Enumeration {
  type T = Value

  val TInt, TFloat, TDate, TString, TBool, TRowId, TAny = Value

  def toString(t: T) = t match {
    case TInt => "int"
    case TFloat => "decimal"
    case TDate => "date"
    case TString => "string"
    case TBool => "bool"
    case TRowId => "rowid"
    case TAny => throw new SQLException("Unable to produce string of type TAny");
  }

  def fromString(t: String) = t.toLowerCase match {
    case "int"    => Type.TInt
    case "float"  => Type.TFloat
    case "decimal"  => Type.TFloat
    case "date"   => Type.TDate
    case "string" => Type.TString
    case "bool"   => Type.TBool
    case "rowid"  => Type.TRowId
  }
}

import Type._

abstract class Expression { 
  def exprType(bindings: Map[String,Type.T]): Type.T
  def exprType: Type.T = exprType(Map[String,Type.T]())
  def children: List[Expression] 
  def rebuild(c: List[Expression]): Expression
}

abstract class LeafExpression extends Expression {
  def children = List[Expression]();
  def rebuild(c: List[Expression]):Expression = { return this }
}

abstract class PrimitiveValue(t: Type.T) 
  extends LeafExpression 
{
  def exprType(x: Map[String,Type.T]) = t
  def asLong: Long;
  def asDouble: Double;
  def asString: String;
  def payload: Object;
}
case class IntPrimitive(v: Long) 
  extends PrimitiveValue(TInt) 
{
  override def toString() = v.toString
  def asLong: Long = v;
  def asDouble: Double = v.toDouble;
  def asString: String = v.toString;
  def payload: Object = v.asInstanceOf[Object];
}
case class StringPrimitive(v: String) 
  extends PrimitiveValue(TString)
{
  override def toString() = "'"+v.toString+"'"
  def asLong: Long = java.lang.Long.parseLong(v)
  def asDouble: Double = java.lang.Double.parseDouble(v)
  def asString: String = v;
  def payload: Object = v.asInstanceOf[Object];
}
case class RowIdPrimitive(v: String)
  extends PrimitiveValue(TRowId)
{
  override def toString() = "'"+v.toString+"'"
  def asLong: Long = java.lang.Long.parseLong(v)
  def asDouble: Double = java.lang.Double.parseDouble(v)
  def asString: String = v;
  def payload: Object = v.asInstanceOf[Object];
}
case class FloatPrimitive(v: Double) 
  extends PrimitiveValue(TFloat)
{
  override def toString() = v.toString
  def asLong: Long = throw new TypeException(TFloat, TInt, "Cast");
  def asDouble: Double = v
  def asString: String = v.toString;
  def payload: Object = v.asInstanceOf[Object];
}
case class DatePrimitive(y: Int, m: Int, d: Int) 
  extends PrimitiveValue(TDate)
{
  override def toString() = "DATE '"+y+"-"+m+"-"+d+"'"
  def asLong: Long = throw new TypeException(TString, TInt, "Cast");
  def asDouble: Double = throw new TypeException(TString, TFloat, "Cast");
  def asString: String = toString;
  def payload: Object = (y, m, d).asInstanceOf[Object];
  def compare(c: DatePrimitive): Integer = {
    if(c.y < y){ -1 }
    else if(c.y > y) { 1 }
    else if(c.m < m) { -1 }
    else if(c.m > m) { 1 }
    else if(c.d < d) { -1 }
    else if(c.d > d) { 1 }
    else { 0 }
  }
}
case class BoolPrimitive(v: Boolean)
  extends PrimitiveValue(TBool)
{
  override def toString() = if(v) {"TRUE"} else {"FALSE"}
  def asLong: Long = throw new TypeException(TBool, TInt, "Cast");
  def asDouble: Double = throw new TypeException(TBool, TFloat, "Cast");
  def asString: String = toString;
  def payload: Object = v.asInstanceOf[Object];
}
case class NullPrimitive()
  extends PrimitiveValue(TAny)
{
  override def toString() = "NULL"
  def asLong: Long = throw new TypeException(TAny, TInt, "Cast Null");
  def asDouble: Double = throw new TypeException(TAny, TFloat, "Cast Null");
  def asString: String = throw new TypeException(TAny, TString, "Cast Null");
  def payload: Object = null
}
case class VarSeedPrimitive(v: Long)
  extends PrimitiveValue(TInt)
{
  VarSeed.setSeed(v)
  override def toString = VarSeed.getSeed.toString
  def asLong: Long = VarSeed.getSeed
  def asDouble: Double = VarSeed.getSeed.toDouble
  def asString: String = VarSeed.getSeed.toString
  def payload: Object = VarSeed.getSeed.asInstanceOf[Object]
}
object VarSeed {
  var seed: Long = 1
  def increment(): Unit = seed += 1
  def setSeed(v: Long) = seed = v
  def getSeed: Long = seed
}

case class Not(child: Expression) 
  extends Expression 
{
  def exprType(bindings: Map[String,Type.T]): Type.T = {
    Arith.escalateCompat(TBool, child.exprType(bindings))
  }
  def children: List[Expression] = List[Expression](child)
  def rebuild(x: List[Expression]): Expression = Not(x(0))
}

abstract class Proc(args: List[Expression]) extends Expression
{
  def getArgs = args
  def children = args
  def get(v: List[PrimitiveValue]): PrimitiveValue
}

object Arith extends Enumeration {
  type Op = Value
  val Add, Sub, Mult, Div, And, Or = Value
  
  def matchRegex = """\+|-|\*|/|\||&""".r
  def fromString(a: String) = {
    a match {
      case "+" => Add
      case "-" => Sub
      case "*" => Mult
      case "/" => Div
      case "&" => And
      case "|" => Or
      case x => throw new Exception("Invalid operand '"+x+"'")
    }
  }

  def escalateNumeric(a: Type.T, b: Type.T): Type.T = {
    (a,b) match {
      case (_, TAny) => a
      case (TAny, _) => b
      case (TInt, TInt) => TInt
      case (TFloat, TInt) => TFloat
      case (TInt, TFloat) => TFloat
      case (TFloat, TFloat) => TFloat
      case ((TInt | TFloat), _) => 
        throw new TypeException(b, TFloat, "Numeric")
      case _ => 
        throw new TypeException(a, TFloat, "Numeric")
    }
  }
  
  def escalateCompat(a: Type.T, b: Type.T): Type.T = {
    (a, b) match {
      case (TAny, _) => b
      case (_, TAny) => a
      case (TInt, TInt) => TInt
      case (TInt, TFloat) => TFloat
      case (TFloat, (TInt | TFloat)) => TFloat
      case (TString, TString) => TString
      case (TRowId, TString) => TString
      case (TBool, TBool) => TBool
      case _ => 
        throw new TypeException(a, b, "Compat")
    }
  }

  def computeType(v: Op, a: Type.T, b: Type.T): Type.T = {
    v match { 
      case (Add | Sub | Mult | Div) => escalateNumeric(a, b)
      case (And | Or) => 
        if(a != TBool) { 
          throw new TypeException(a, TBool, "BoolOp")
        } else if(b != TBool) {
          throw new TypeException(b, TBool, "BoolOp")
        } else {
          TBool
        }
    }
  }
  def opString(v: Op): String = {
    v match {
      case Add => "+"
      case Sub => "-"
      case Mult => "*"
      case Div => "/"
      case And => " AND "
      case Or => " OR "
    }
  }
  def isBool(v: Op): Boolean = {
    v match {
      case And | Or => true
      case _ => false
    }
  }
  def isNumeric(v: Op): Boolean = !isBool(v)

  def makeAnd(a: Expression, b: Expression): Expression = 
    (a, b) match {
      case (BoolPrimitive(true), _) => b
      case (_, BoolPrimitive(true)) => a
      case (BoolPrimitive(false), _) => BoolPrimitive(false)
      case (_, BoolPrimitive(false)) => BoolPrimitive(false)
      case _ => Arithmetic(And, a, b)
    }
  def makeOr(a: Expression, b: Expression): Expression = 
  (a, b) match {
    case (BoolPrimitive(false), _) => b
    case (_, BoolPrimitive(false)) => a
    case (BoolPrimitive(true), _) => BoolPrimitive(true)
    case (_, BoolPrimitive(true)) => BoolPrimitive(true)
    case _ => Arithmetic(Or, a, b)
  }
  def makeNot(e: Expression): Expression =
  {
    e match {
      case BoolPrimitive(b) => BoolPrimitive(!b)
      case Arithmetic(And, a, b) =>
        Arithmetic(Or, makeNot(a), makeNot(b))
      case Arithmetic(Or, a, b) =>
        Arithmetic(And, makeNot(a), makeNot(b))
      case Comparison(c, a, b) =>
        Comparison(Cmp.negate(c), a, b)
      case IsNullExpression(a, n) =>
        IsNullExpression(a, !n)
      case Not(a) => a
      case _ => Not(e)
    }
  }
}

object Cmp extends Enumeration {
  type Op = Value
  val Eq, Neq, Gt, Lt, Gte, Lte, Like, NotLike = Value
  
  def computeType(v: Op, a: Type.T, b: Type.T): Type.T = {
    v match {
      case (Eq | Neq) => 
        Arith.escalateCompat(a, b); return TBool
      case (Gt | Gte | Lt | Lte) => 
        Arith.escalateNumeric(a, b); return TBool
      case (Like | NotLike) => 
        if(a != TString) { 
          throw new TypeException(a, TBool, "Like")
        } else if(b != TString) {
          throw new TypeException(b, TBool, "Like")
        } else {
          TBool
        }
    }
  }
  
  def negate(v: Op): Op = {
    v match {
      case Eq => Neq
      case Neq => Eq
      case Gt => Lte
      case Gte => Lt
      case Lt => Gte
      case Lte => Gt
    }
  }
  
  def opString(v: Op): String = {
    v match {
      case Eq => "="
      case Neq => "<>"
      case Gt => ">"
      case Gte => ">="
      case Lt => "<"
      case Lte => "<="
      case Like => " LIKE "
      case NotLike => " NOT LIKE "
    }
  }
}

case class Var(name: String) extends LeafExpression {
  def exprType(bindings: Map[String,Type.T]): T = {
    val t = bindings.get(name)
    if(t.isEmpty){
      throw new RAException("Missing Variable '" + name + "' in "+bindings.toString)
    }
    t.get
  }
  override def toString = name;
}

case class Arithmetic(op: Arith.Op, lhs: Expression, 
                      rhs: Expression) 
	extends Expression 
{
  def exprType(bindings: Map[String,Type.T]): T = {
    Arith.computeType(op, 
      lhs.exprType(bindings), 
      rhs.exprType(bindings)
    )
  }
  override def toString() = 
	" (" + lhs.toString + Arith.opString(op) + rhs.toString + ") "
  def children = List(lhs, rhs)
  def rebuild(c: List[Expression]) = Arithmetic(op, c(0), c(1))
}
case class Comparison(op: Cmp.Op, lhs: Expression, 
                      rhs: Expression) 
	extends Expression 
{
  def exprType(bindings: Map[String,Type.T]): T = {
    Cmp.computeType(op, 
      lhs.exprType(bindings), 
      rhs.exprType(bindings)
    )
  }
  override def toString() = 
	" (" + lhs.toString + Cmp.opString(op) + rhs.toString + ") "
  def children = List(lhs, rhs)
  def rebuild(c: List[Expression]) = Comparison(op, c(0), c(1))
}
case class Function(op: String, params: List[Expression]) extends Expression {
  def exprType(bindings: Map[String, Type.T]): T = {
    op match {
      case "JOIN_ROWIDS" => TRowId
      case CTables.ROW_PROBABILITY => TString
      case _ => bindings.get("__"+op+"()").get
    }
  }
  override def toString() = {
    op match {
      // Need to special case COUNT DISTINCT
      case "COUNT" if params.size > 0 => 
            "COUNT(DISTINCT " + params.map( _.toString ).mkString(", ") + ")"
      case "COUNT" if params.size == 0 => 
            "COUNT(*)"
      case "EXTRACT" =>
        op + "(" + params(0).asInstanceOf[StringPrimitive].v + " FROM " + 
          params(1).toString + ")"
      case _ => op + "(" + params.map( _.toString ).mkString(", ") + ")"
    }
  }
  def children = params
  def rebuild(c: List[Expression]) = Function(op, c)
}
case class WhenThenClause(when: Expression, 
                          then: Expression) 
{
  def exprType(bindings: Map[String,Type.T]): T = {
    if(when.exprType(bindings) != TBool){
      throw new TypeException(when.exprType, TBool, "WHEN")
    }
    return then.exprType(bindings)
  }
  override def toString() = "WHEN " + when.toString + " THEN " + then.toString
}
case class CaseExpression(
  whenClauses: List[WhenThenClause], 
  elseClause: Expression
) extends Expression 
{
  def exprType(bindings: Map[String,Type.T]): T = {
    whenClauses.
      map ( _.exprType(bindings) ).
      fold(TAny)( Arith.escalateCompat(_,_) )
  }
  override def toString() = 
	"CASE "+whenClauses.map( _.toString ).mkString(" ")+
	" ELSE "+elseClause.toString+" END"
  def children = 
	whenClauses.map( (w) => List(w.when, w.then) ).flatten ++ List(elseClause)
  def rebuild(c: List[Expression]) = {
  	var currC = c
  	val w =
  	  whenClauses.map ( _ => {
  		currC match { 
  		  case w :: t :: rest =>
      		currC = rest 
      		WhenThenClause(w, t)
  		  case _ => 
    			throw new SQLException("Invalid Rebuild of a Case: "+c)
  		}
	  })
	CaseExpression(w, currC(0))
  }
}
case class IsNullExpression(child: Expression, neg: Boolean = false) extends Expression { 
  def exprType(bindings: Map[String, Type.T]): T = {
    child.exprType(bindings);
    TBool
  }
  override def toString() = {child.toString+" IS"+(if(neg){" NOT"}else{""})+" NULL"}
  def children = List(child)
  def rebuild(c: List[Expression]) = IsNullExpression(c(0), neg)
}