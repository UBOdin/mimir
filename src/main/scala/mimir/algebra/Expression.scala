package mimir.algebra;

import mimir.algebra.Type._;
import org.joda.time.DateTime;
import org.joda.time.Period;

/**
 * Base type for expression trees.  Represents a single node in the tree.
 */
abstract sealed class Expression extends Serializable with ExpressionConstructors { 
  /**
   * Return all of the children of the current tree node
   */
  def children: Seq[Expression] 

  /**
   * Return this
   */
  def toExpression = this

  /**
   * Return a new instance of the same object, but with the 
   * children replaced with the provided list.  The list must
   * be of the same size returned by children.  This is mostly
   * to facilitate recur, below
   */
  def rebuild(c: Seq[Expression]): Expression
  /**
   * Perform a recursive rewrite.  
   * The following pattern is pretty common throughout Mimir:
   * def replaceFooWithBar(e:Expression): Expression =
   *   e match {
   *     case Foo(a, b, c, ...) => Bar(a, b, c, ...)
   *     case _ => e.recur(replaceFooWithBar(_))
   *   }
   * Note how specific rewrites are applied to specific patterns
   * in the tree, and recur is used to ignore/descend through 
   * every other class of object
   */
  def recur(f: Expression => Expression) =
    rebuild(children.map(f))
}

/**
 * Slightly more specific base type for nodes without children
 * Like Expression, but handles children/rebuild for free
 */
@SerialVersionUID(100L)
sealed abstract class LeafExpression extends Expression {
  def children = List[Expression]();
  def rebuild(c: Seq[Expression]):Expression = { return this }
}

/////////////// Computations ///////////////

/**
 * Boolean Negation
 */
@SerialVersionUID(100L)
case class Not(child: Expression) 
  extends Expression 
{
  def children: Seq[Expression] = List[Expression](child)
  def rebuild(x: Seq[Expression]): Expression = Not(x(0))
  override def toString = ("NOT(" + child.toString + ")")
}

/**
 * Utility class supporting binary arithmetic operations
 * 
 * Arith.Op is an Enumeration type for binary arithmetic operations
 */
object Arith extends Enumeration {
  type Op = Value
  val Add, Sub, Mult, Div, And, Or, BitAnd, BitOr, ShiftLeft, ShiftRight = Value
  
  /**
   * Regular expresion to match any and all binary operations
   */
  def matchRegex = """\+|-|\*|/|\|\||&&|\||&""".r

  /**
   * Convert from the operator's string encoding to its Arith.Op rep
   */
  def fromString(a: String) = {
    a match {
      case "+" => Add
      case "-" => Sub
      case "*" => Mult
      case "/" => Div
      case "&" => BitAnd
      case "|" => BitOr
      case "<<" => ShiftLeft
      case ">>" => ShiftRight
      case "&&" => And
      case "||" => Or
      case x => throw new Exception("Invalid operand '"+x+"'")
    }
  }
  /**
   * Convert from the operator's Arith.Op representation to a string
   */
  def opString(v: Op): String = 
  {
    v match {
      case Add => "+"
      case Sub => "-"
      case Mult => "*"
      case Div => "/"
      case BitAnd => " & "
      case BitOr => " | "
      case ShiftLeft => " << "
      case ShiftRight => " >> "
      case And => " AND "
      case Or => " OR "
    }
  }
  /**
   * Is this binary operation a boolean operator (AND/OR)
   */
  def isBool(v: Op): Boolean = 
  {
    v match {
      case And | Or => true
      case _ => false
    }
  }

  /**
   * Is this binary operation a numeric operator (+, -, *, /, & , |)
   */
  def isNumeric(v: Op): Boolean = !isBool(v)

}

/**
 * Enumerator for comparison types
 */
object Cmp extends Enumeration {
  type Op = Value
  val Eq, Neq, Gt, Lt, Gte, Lte, Like, NotLike = Value
  
  def negate(v: Op): Op = 
  {
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

  def flip(v: Op): Option[Op] = 
  {
    v match {
      case Eq      => Some(Eq)
      case Neq     => Some(Neq)
      case Gt      => Some(Lt)
      case Gte     => Some(Lte)
      case Lt      => Some(Gt)
      case Lte     => Some(Gte)
      case Like    => None
      case NotLike => None
    }
  }
  
  def opString(v: Op): String = 
  {
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

/**
 * Non-relational binary operations.  This includes integer and boolean
 * arithmetic.
 * 
 * See the Arith enum above for a full list of available operations.
 */
@SerialVersionUID(100L)
case class Arithmetic(op: Arith.Op, lhs: Expression, 
                      rhs: Expression) 
	extends Expression 
{
  override def toString() = 
	" (" + lhs.toString + Arith.opString(op) + rhs.toString + ") "
  def children = List(lhs, rhs)
  def rebuild(c: Seq[Expression]) = Arithmetic(op, c(0), c(1))
}

/**
 * Relational binary operations.  This includes equality/inequality,
 * ordering operators, and simple string comparators (e.g., SQL's LIKE)
 */
@SerialVersionUID(100L)
case class Comparison(op: Cmp.Op, lhs: Expression, 
                      rhs: Expression) 
	extends Expression 
{
  override def toString() = 
	" (" + lhs.toString + Cmp.opString(op) + rhs.toString + ") "
  def children = List(lhs, rhs)
  def rebuild(c: Seq[Expression]) = Comparison(op, c(0), c(1))
}

/**
 * Invocation of a System- or User-Defined Function.  
 * 
 * For Mimir to be completely happy, functions need to be defined
 * with mimir.algebra.FunctionRegistry and have an entry in 
 * mimir.algebra.Eval.  
 *
 * TODO: Move inline function definition from Eval to 
 *       FunctionRegistry
 */
@SerialVersionUID(100L)
case class Function(op: String, params: Seq[Expression]) extends Expression {
  override def toString() = {
    op match {
      // Need to special case COUNT DISTINCT
      // OK: Is this actually needed?  This should be part of the Aggregate operator, 
      //     and not Function.
      case "COUNT" if params.size > 0 => 
            "COUNT(DISTINCT " + params.map( _.toString ).mkString(", ") + ")"
      case "COUNT" if params.size == 0 => 
            "COUNT(*)"

      case _ => op + "(" + params.map( _.toString ).mkString(", ") + ")"
    }
  }
  def children = params
  def rebuild(c: Seq[Expression]) = Function(op, c)
}

/**
 * Representation of a column reference (a SQL variable).  Names are all that's
 * needed.  Typechecker expects Operators to be self-contained, but not 
 * Expressions.
 */
@SerialVersionUID(100L)
case class Var(name: String) extends LeafExpression {
  override def toString = name;
}

/**
 * Representation of a Provenance Token / Row Identifier.  RowId has a special
 * place in the expression syntax, because unlike classical SQL implicit ROWID
 * attributes, Mimir's ROWID attributes don't need to reference a specific
 * table.  If a ROWID appears in an operator that reads from multiple tables,
 * Mimir will synthesize a new, unique ROWID from the ROWIDs of its constituents.
 * 
 * see mimir.provenance.Provenance for more details.
 */
@SerialVersionUID(100L)
case class RowIdVar() extends LeafExpression
{
  override def toString = "ROWID";
}

/**
 * Representation of a JDBC Variable.
 */
@SerialVersionUID(100L)
case class JDBCVar(t: Type) extends LeafExpression
{
  override def toString = "?";
}

/**
 * Representation of an If-Then-Else block.  Note that this differs from
 * SQL's use of CASE blocks.  If-Then-Else is substantially easier to work
 * with for recursive analyses.  
 *
 * For conversion between If-Then-Else and CASE semantics, see the methods
 *  - makeCaseExpression
 *  - foldConditionalsToCase
 * in mimir.algebra.ExpressionUtils.
 */
@SerialVersionUID(100L)
case class Conditional(condition: Expression, thenClause: Expression,
                       elseClause: Expression) extends Expression 
{
  override def toString() = 
  	"IF "+condition.toString+" THEN "+thenClause.toString+
    " ELSE "+elseClause.toString+" END"
  def children = List(condition, thenClause, elseClause)
  def rebuild(c: Seq[Expression]) = {
    Conditional(c(0), c(1), c(2))
  }
}

/**
 * Representation of a unary IS NULL
 */
@SerialVersionUID(100L)
case class IsNullExpression(child: Expression) extends Expression { 
  override def toString() = {child.toString+" IS NULL"}
  def children = List(child)
  def rebuild(c: Seq[Expression]) = IsNullExpression(c(0))
}

/////////////// Primitive Values ///////////////


/**
 * Slightly more specific base type for constant terms.  PrimitiveValue
 * also acts as a boxing type for constants in Mimir.
 */
abstract sealed class PrimitiveValue(t: Type)
  extends LeafExpression with Serializable
{
  def getType = t
  /**
   * Convert the current object into a long or throw a TypeException if 
   * not possible
   */
  def asLong: Long;
  /**
   * Convert the current object into an int or throw a TypeException if
   * not possible
   */
  def asInt: Int = asLong.toInt
  /**
   * Convert the current object into a double or throw a TypeException if 
   * not possible
   */
  def asDouble: Double;
  /**
   * Convert the current object into a float or throw a TypeException if 
   * not possible
   */
  def asFloat: Float = asDouble.toFloat
  /**
   * Convert the current object into a boolean or throw a TypeException if 
   * not possible
   */
  def asBool: Boolean;
  /**
   * Convert the current object into a DateTime or throw a TypeException if
   * not possible
   */
  def asDateTime: DateTime;
  /**
   * Convert the current object into a string or throw a TypeException if 
   * not possible.  Note the difference between this and toString.
   * asString returns the content, while toString returns a representation
   * of the primitive value itself.
   * An overt example of this is:
   *   val temp = StringPrimitive('foo')
   *   println(temp.asString)  // Returns "foo"
   *   println(temp.toString)  // Returns "'foo'"
   * Note the extra quotes.  If you ever see a problem involving strings
   * with ''too many nested quotes'', your problem is probably with asString
   */
  def asString: String;
  /**
   * return the contents of the variable as just an object.
   */
  def asInterval: Period;
  /**
   * return the contents of the variable as just an object.
   */
  def payload: Object;
  /**
   * Execute the nested expression only if the value is not null.  Otherwise
   * return null
   */
  def ifNotNull(cmd: (PrimitiveValue => PrimitiveValue)) = cmd(this)
}

abstract sealed class NumericPrimitive(t: Type) extends PrimitiveValue(t)

/**
 * Boxed representation of a long integer
 */
@SerialVersionUID(100L)
case class IntPrimitive(v: Long) 
  extends NumericPrimitive(TInt())
{
  override def toString() = v.toString
  def asLong: Long = v;
  def asDouble: Double = v.toDouble;
  def asBool: Boolean = throw new TypeException(TInt(), TBool(), "Hard Cast")
  def asDateTime: DateTime = throw new TypeException(TInt(), TDate(), "Hard Cast")
  def asString: String = v.toString;
  def asInterval: Period = throw new TypeException(TInt(), TInterval(), "Hard Cast")
  def payload: Object = v.asInstanceOf[Object];
}
/**
 * Boxed representation of a string
 */
@SerialVersionUID(100L)
case class StringPrimitive(v: String) 
  extends PrimitiveValue(TString())
{
  override def toString() = "'"+v.toString+"'"
  def asLong: Long = java.lang.Long.parseLong(v)
  def asDouble: Double = java.lang.Double.parseDouble(v)
  def asBool: Boolean = throw new TypeException(TString(), TBool(), "Hard Cast")
  def asDateTime: DateTime = throw new TypeException(TString(), TDate(), "Hard Cast")
  def asString: String = v;
  def asInterval: Period = throw new TypeException(TString(), TInterval(), "Hard Cast")
  def payload: Object = v.asInstanceOf[Object];
}
/**
 * Boxed representation of a type object
 */
@SerialVersionUID(100L)
case class TypePrimitive(t: Type)
  extends PrimitiveValue(TType())
{
  override def toString() = t.toString
  def asLong: Long = throw new TypeException(TType(), TInt(), "Hard Cast")
  def asDouble: Double = throw new TypeException(TType(), TFloat(), "Hard Cast")
  def asBool: Boolean = throw new TypeException(TType(), TBool(), "Hard Cast")
  def asDateTime: DateTime = throw new TypeException(TType(), TDate(), "Hard Cast")
  def asString: String = t.toString;
  def asInterval: Period = throw new TypeException(TType(), TInterval(), "Hard Cast")
  def payload: Object = t.asInstanceOf[Object];
}
/**
 * Boxed representation of a row identifier/provenance token
 */
@SerialVersionUID(100L)
case class RowIdPrimitive(v: String)
  extends PrimitiveValue(TRowId())
{
  override def toString() = "'"+v.toString+"'"
  def asLong: Long = java.lang.Long.parseLong(v)
  def asDouble: Double = java.lang.Double.parseDouble(v)
  def asBool: Boolean = throw new TypeException(TRowId(), TBool(), "Hard Cast")
  def asDateTime: DateTime = throw new TypeException(TRowId(), TDate(), "Hard Cast")
  def asString: String = v;
  def asInterval: Period = throw new TypeException(TRowId(), TInterval(), "Hard Cast")
  def payload: Object = v.asInstanceOf[Object];
}
/**
 * Boxed representation of a double-precision floating point number
 */
@SerialVersionUID(100L)
case class FloatPrimitive(v: Double) 
  extends NumericPrimitive(TFloat())
{
  override def toString() = v.toString
  def asLong: Long = throw new TypeException(TFloat(), TInt(), "Hard Cast");
  def asDouble: Double = v
  def asBool: Boolean = throw new TypeException(TFloat(), TBool(), "Hard Cast")
  def asDateTime: DateTime = throw new TypeException(TFloat(), TDate(), "Hard Cast")
  def asString: String = v.toString;
  def asInterval: Period = throw new TypeException(TFloat(), TInterval(), "Hard Cast")
  def payload: Object = v.asInstanceOf[Object];
}

/**
 * Boxed representation of a date
 */
@SerialVersionUID(100L)
case class DatePrimitive(y: Int, m: Int, d: Int)
  extends PrimitiveValue(TDate()) with Comparable[DatePrimitive]
{
  override def toString() = s"DATE '${asString}'"
  def asLong: Long = throw new TypeException(TDate(), TInt(), "Hard Cast");
  def asDouble: Double = throw new TypeException(TDate(), TFloat(), "Hard Cast");
  def asBool: Boolean = throw new TypeException(TDate(), TBool(), "Hard Cast")
  def asString: String = f"$y%04d-$m%02d-$d%02d"
  def asInterval: Period = throw new TypeException(TDate(), TInterval(), "Hard Cast")
  def payload: Object = (y, m, d).asInstanceOf[Object];
  final def compareTo(c: DatePrimitive): Int = {
    if(c.y < y){ -1 }
    else if(c.y > y) { 1 }
    else if(c.m < m) { -1 }
    else if(c.m > m) { 1 }
    else if(c.d < d) { -1 }
    else if(c.d > d) { 1 }
    else { 0 }
  }

  def >(c:DatePrimitive): Boolean = compareTo(c) > 0
  def >=(c:DatePrimitive): Boolean = compareTo(c) >= 0
  def <(c:DatePrimitive): Boolean = compareTo(c) < 0
  def <=(c:DatePrimitive): Boolean = compareTo(c) <= 0

  def asDateTime: DateTime = new DateTime(y, m, d, 0, 0)
}

/**
  *
  * Boxed Representation of Timestamp
  */
@SerialVersionUID(100L)
case class TimestampPrimitive(y: Int, m: Int, d: Int, hh: Int, mm: Int, ss: Int, ms: Int)
  extends PrimitiveValue(TTimestamp()) with Comparable[TimestampPrimitive]
{
  override def toString() = s"DATE '${asString}'"
  def asLong: Long = throw new TypeException(TDate(), TInt(), "Hard Cast");
  def asDouble: Double = throw new TypeException(TDate(), TFloat(), "Hard Cast");
  def asBool: Boolean = throw new TypeException(TDate(), TBool(), "Hard Cast")
  def asString: String = f"$y%04d-$m%02d-$d%02d $hh%02d:$mm%02d:$ss%02d.$ms%04d"
  def asInterval: Period = throw new TypeException(TDate(), TInterval(), "Hard Cast")
  def payload: Object = (y, m, d).asInstanceOf[Object];
  final def compareTo(c: TimestampPrimitive): Int = {
    if(c.y < y){ -1 }
    else if(c.y > y) { 1 }
    else if(c.m < m) { -1 }
    else if(c.m > m) { 1 }
    else if(c.d < d) { -1 }
    else if(c.d > d) { 1 }
    else if(c.hh < hh) { -1 }
    else if(c.hh > hh) { 1 }
    else if(c.mm < mm) { -1 }
    else if(c.mm > mm) { 1 }
    else if(c.ss < ss) { -1 }
    else if(c.ss > ss) { 1 }
    else { 0 }
  }

  def >(c:TimestampPrimitive): Boolean = compareTo(c) > 0
  def >=(c:TimestampPrimitive): Boolean = compareTo(c) >= 0
  def <(c:TimestampPrimitive): Boolean = compareTo(c) < 0
  def <=(c:TimestampPrimitive): Boolean = compareTo(c) <= 0

  def asDateTime: DateTime = new DateTime(y, m, d, hh, mm, ss, ms)
}
/**
 * Boxed representation of a boolean
 */
@SerialVersionUID(100L)
case class BoolPrimitive(v: Boolean)
  extends PrimitiveValue(TBool())
{
  override def toString() = if(v) {"TRUE"} else {"FALSE"}
  def asLong: Long = throw new TypeException(TBool(), TInt(), "Hard Cast");
  def asDouble: Double = throw new TypeException(TBool(), TFloat(), "Hard Cast");
  def asDateTime: DateTime = throw new TypeException(TBool(), TDate(), "Hard Cast")
  def asBool: Boolean = v
  def asString: String = toString;
  def asInterval: Period = throw new TypeException(TBool(), TInterval(), "Hard Cast")
  def payload: Object = v.asInstanceOf[Object];
}
/**
 * Boxed representation of NULL
 */
@SerialVersionUID(100L)
case class NullPrimitive()
  extends PrimitiveValue(TAny())
{
  override def toString() = "NULL"
  def asLong: Long = throw new NullTypeException(TAny(), TInt(), "Hard Cast Null");
  def asDouble: Double = throw new NullTypeException(TAny(), TFloat(), "Hard Cast Null");
  def asString: String = throw new NullTypeException(TAny(), TString(), "Hard Cast Null");
  def asBool: Boolean = throw new NullTypeException(TAny(), TBool(), "Hard Cast Null")
  def asDateTime: DateTime = throw new NullTypeException(TAny(), TDate(), "Hard Cast")
  def asInterval: Period = throw new TypeException(TAny(), TInterval(), "Hard Cast")
  def payload: Object = null
  override def ifNotNull(cmd: (PrimitiveValue => PrimitiveValue)) = this
}


/**
  *
  * Boxed Representation of Interval
  */
@SerialVersionUID(100L)
case class IntervalPrimitive(p: Period)
  extends PrimitiveValue(TInterval())
{
  override def toString() = s"INTERVAL '${asString}'"
  def asLong: Long = throw new TypeException(TInterval(), TInt(), "Hard Cast");
  def asDouble: Double = throw new TypeException(TInterval(), TFloat(), "Hard Cast");
  def asBool: Boolean = throw new TypeException(TInterval(), TBool(), "Hard Cast")
  def asString: String = p.toString
  def payload: Object = p;
  def asDateTime: DateTime = throw new TypeException(TInterval(), TDate(), "Hard Cast")
  def asInterval: Period = p
}

/////////////// Special Expression Types ///////////////

/**
 * A placeholder for use in extending Eval;  A proc is an expression that 
 * can be evaluated, but is not itself part of mimir's grammar.
 * 
 * The proc defines the method of evaluation.
 */
abstract class Proc(val args: Seq[Expression]) extends Expression
{
  def getType(argTypes: Seq[Type]): Type
  def getArgs = args
  def children = args
  def get(v: Seq[PrimitiveValue]): PrimitiveValue
}

sealed abstract class UncertaintyCausingExpression extends Expression
{ 
  def name: String
}

/**
 * VGTerms are the building block of incomplete data in Mimir.  A VGTerm
 * is linked to a specific model, which defines its behavior.
 * 
 * Each distinct value of { name x idx x args } identifies *one* variable.
 */
case class VGTerm(
  name: String, 
  idx: Int,
  args: Seq[Expression],
  hints: Seq[Expression]
) extends UncertaintyCausingExpression {
  override def toString() = "{{ "+name+";"+idx+"["+args.mkString(", ")+"]["+hints.mkString(", ")+"] }}"
  override def children: Seq[Expression] = args ++ hints
  override def rebuild(x: Seq[Expression]) = {
    val (a, h) = x.splitAt(args.length)
    VGTerm(name, idx, a, h)
  }
  def isDataDependent: Boolean = args.size > 0
}

/**
 * Warnings are a "light" form of VGTerms.  Like VGTerms warnings "flag" 
 * their output in a way that propagates along with dependencies, but 
 * unlike VGTerms, a warning has no alternative values associated with it.
 * It doesn't signify a random decision, but rather uncertainty that the
 * value that gets passed through is in fact correct.
 */
case class DataWarning(
  name: String,
  value: Expression,
  message: Expression, 
  key: Seq[Expression]
) extends UncertaintyCausingExpression {
  override def toString() = s"($name(${(key :+ message).mkString(", ")}))@($value)"
  override def children: Seq[Expression] = Seq(value, message) ++ key
  override def rebuild(x: Seq[Expression]) = {
    DataWarning(name, x(0), x(1), x.tail.tail)
  }
}