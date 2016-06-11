package mimir.algebra;

import java.sql._

import mimir.ctables.CTables

case class TypeException(found: Type.T, expected: Type.T, 
                    context:String) 
  extends Exception(
    "Type Mismatch ["+context+
    "]: found "+found.toString+
    ", but expected "+expected.toString
  );
class RAException(msg: String) extends Exception(msg);

object Type extends Enumeration {
  type T = Value//where does Value come from?

  val TInt, TFloat, TDate, TString, TBool, TRowId, TType, TAny = Value

  def toString(t: T) = t match {
    case TInt => "int"
    case TFloat => "decimal"
    case TDate => "date"
    case TString => "string"
    case TBool => "bool"
    case TRowId => "rowid"
    case TType => "type"
    case TAny => "any"//throw new SQLException("Unable to produce string of type TAny");
  }

  def toStringPrimitive(t: T) = StringPrimitive(toString(t))

  def fromString(t: String) = t.toLowerCase match {
    case "int"     => Type.TInt
    case "integer" => Type.TInt
    case "float"   => Type.TFloat
    case "decimal" => Type.TFloat
    case "real"    => Type.TFloat
    case "date"    => Type.TDate
    case "varchar" => Type.TString
    case "char"    => Type.TString
    case "string"  => Type.TString
    case "bool"    => Type.TBool
    case "rowid"   => Type.TRowId
    case "type"    => Type.TType
    case _ =>  throw new SQLException("Invalid Type '" + t + "'");
  }

  def fromStringPrimitive(t: StringPrimitive) = fromString(t.asString)
}

import mimir.algebra.Type._

abstract class Expression { 
  def children: List[Expression] 
  def rebuild(c: List[Expression]): Expression
  def recur(f: Expression => Expression) =
    rebuild(children.map(f))
}

abstract class LeafExpression extends Expression {
  def children = List[Expression]();
  def rebuild(c: List[Expression]):Expression = { return this }
}

abstract class PrimitiveValue(t: Type.T) 
  extends LeafExpression 
{
  def getType = t
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
case class KeywordPrimitive(v: String, t: Type.T)
  extends PrimitiveValue(t)
{
  override def toString() = v
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

case class Not(child: Expression) 
  extends Expression 
{
  def children: List[Expression] = List[Expression](child)
  def rebuild(x: List[Expression]): Expression = Not(x(0))
}

abstract class Proc(args: List[Expression]) extends Expression
{
  def getType(argTypes: List[Type.T]): Type.T
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
  def makeAnd(el: List[Expression]): Expression = 
    el.foldLeft[Expression](BoolPrimitive(true))(makeAnd(_,_))
  def makeOr(a: Expression, b: Expression): Expression = 
  (a, b) match {
    case (BoolPrimitive(false), _) => b
    case (_, BoolPrimitive(false)) => a
    case (BoolPrimitive(true), _) => BoolPrimitive(true)
    case (_, BoolPrimitive(true)) => BoolPrimitive(true)
    case _ => Arithmetic(Or, a, b)
  }
  def makeOr(el: List[Expression]): Expression = 
    el.foldLeft[Expression](BoolPrimitive(false))(makeOr(_,_))
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
      case Not(a) => a
      case _ => Not(e)
    }
  }
  def getConjuncts(e: Expression): List[Expression] = {
    e match {
      case BoolPrimitive(true) => List[Expression]()
      case Arithmetic(And, a, b) => 
        getConjuncts(a) ++ getConjuncts(b)
      case _ => List(e)
    }
  }
  def getDisjuncts(e: Expression): List[Expression] = {
    e match {
      case BoolPrimitive(false) => List[Expression]()
      case Arithmetic(Or, a, b) => 
        getConjuncts(a) ++ getConjuncts(b)
      case _ => List(e)
    }
  }
}

object Cmp extends Enumeration {
  type Op = Value
  val Eq, Neq, Gt, Lt, Gte, Lte, Like, NotLike = Value
  
  def negate(v: Op): Op = {
    v match {
      case Eq => Neq
      case Neq => Eq
      case Gt => Lte
      case Gte => Lt
      case Lt => Gte
      case Lte => Gt
      case Like => NotLike
      case NotLike => Like
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
  override def toString = name;
}

case class Arithmetic(op: Arith.Op, lhs: Expression, 
                      rhs: Expression) 
	extends Expression 
{
  override def toString() = 
	" (" + lhs.toString + Arith.opString(op) + rhs.toString + ") "
  def children = List(lhs, rhs)
  def rebuild(c: List[Expression]) = Arithmetic(op, c(0), c(1))
}
case class Comparison(op: Cmp.Op, lhs: Expression, 
                      rhs: Expression) 
	extends Expression 
{
  override def toString() = 
	" (" + lhs.toString + Cmp.opString(op) + rhs.toString + ") "
  def children = List(lhs, rhs)
  def rebuild(c: List[Expression]) = Comparison(op, c(0), c(1))
}
case class Function(op: String, params: List[Expression]) extends Expression {
  override def toString() = {
    op match {
      // Need to special case COUNT DISTINCT
      case "COUNT" if params.size > 0 => 
            "COUNT(DISTINCT " + params.map( _.toString ).mkString(", ") + ")"
      case "COUNT" if params.size == 0 => 
            "COUNT(*)"

      case _ => op + "(" + params.map( _.toString ).mkString(", ") + ")"
    }
  }
  def children = params
  def rebuild(c: List[Expression]) = Function(op, c)
}
case class Conditional(condition: Expression, thenClause: Expression,
                       elseClause: Expression) extends Expression 
{
  override def toString() = 
  	"IF "+condition.toString+" THEN "+thenClause.toString+
    " ELSE "+elseClause.toString+" END"
  def children = List(condition, thenClause, elseClause)
  def rebuild(c: List[Expression]) = {
    Conditional(c(0), c(1), c(2))
  }
}
case class IsNullExpression(child: Expression) extends Expression { 
  override def toString() = {child.toString+" IS NULL"}
  def children = List(child)
  def rebuild(c: List[Expression]) = IsNullExpression(c(0))
}