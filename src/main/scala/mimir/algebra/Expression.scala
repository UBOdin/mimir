package mimir.algebra;

import java.sql._

import mimir.ctables.CTables
import mimir.lenses.TypeList

case class TypeException(found: Type, expected: Type,
                    context:String) 
  extends Exception(
    "Type Mismatch ["+context+
    "]: found "+found.toString+
    ", but expected "+expected.toString
  );
class RAException(msg: String) extends Exception(msg);

/**
 * An enum class defining the type of primitive-valued expressions
 * (e.g., integers, floats, strings, etc...)
 */

sealed trait Type{
  def toString(t:Type) = t match {
    case TInt() => "int"
    case TFloat() => "real"
    case TDate() => "date"
    case TString() => "varchar"
    case TBool() => "bool"
    case TRowId() => "rowid"
    case TType() => "type"
    case TAny() => "any"
    case TUser(name) => name
  }
  def fromString(t: String) = t.toLowerCase match {
    case "int"     => TInt()
    case "integer" => TInt()
    case "float"   => TFloat()
    case "decimal" => TFloat()
    case "real"    => TFloat()
    case "date"    => TDate()
    case "varchar" => TString()
    case "char"    => TString()
    case "string"  => TString()
    case "bool"    => TBool()
    case "rowid"   => TRowId()
    case "type"    => TType()
    case _ =>  throw new SQLException("Invalid Type '" + t + "'");
  }
  override def toString(): String = {
    toString(this)
  }
  def toSQLiteType(i:Int) = i match {
    case 0 => TInt()
    case 1 => TFloat()
    case 2 => TDate()
    case 3 => TString()
    case 4 => TBool()
    case 5 => TRowId()
    case 6 => TType()
    case 7 => TAny()
    case _ => {
      val num = i - 8 // 8 because this is the number of native types, if more are added then this number needs to increase
      if(num < 0){
        throw new Exception("This number is not part of the type: num == " + num.toString())
      }
      var location = 0
      var t:Type = null
      TypeList.typeList.foreach((tuple) => {
        if(location == num){
          t = TUser(tuple._1)
        }
        location += 1
      })
      if(t == null){
        throw new Exception("This number is not part of the type: num == " + num.toString())
      }
      t
    }
  }
  def id(t:Type) = t match {
    case TInt() => 0
    case TFloat() => 1
    case TDate() => 2
    case TString() => 3
    case TBool() => 4
    case TRowId() => 5
    case TType() => 6
    case TAny() => 7
    case TUser(name)  => {
      var location = -1
      var temp = 0
      TypeList.typeList.foreach((tuple) => {
        if(tuple._1.equals(name)){
          location = temp + 8 // 8 because of other types, if more native types are added then you need to increase this number
        }
        temp += 1
      })
      if(location == -1){
        throw new Exception("This number is not part of the type")
      }
      location
    }
  }
}

case class TInt() extends Type
case class TFloat() extends Type
case class TDate() extends Type
case class TString() extends Type
case class TBool() extends Type
case class TRowId() extends Type
case class TType() extends Type
case class TAny() extends Type
case class TUser(name:String) extends Type

/*
object Type extends Enumeration {
  /**
   * The base type of the enum.  Type.T is an instance of Type
   */
  type T = Value
  /**
   * The enum values themselves
   */

  val TInt(), TFloat(), TDate(), TString(), TBool(), TRowId(), TType(), TAny() = Value

  /**
   * Convert a type to a SQL-friendly name
   */
  def toString(t: T) = t match {
    case TInt() => "int"
    case TFloat() => "real"
    case TDate() => "date"
    case TString() => "varchar"
    case TBool() => "bool"
    case TRowId() => "rowid"
    case TType() => "type"
//    case TUser => "User Defined"
    case TAny() => "any"//throw new SQLException("Unable to produce string of type TAny()");
  }

  def toStringPrimitive(t: T) = StringPrimitive(toString(t))

  /**
   * Convert a type from a SQL-friendly name
   */
  def fromString(t: String) = t.toLowerCase match {
    case "int"     => Type.TInt()
    case "integer" => Type.TInt()
    case "float"   => Type.TFloat()
    case "decimal" => Type.TFloat()
    case "real"    => Type.TFloat()
    case "date"    => Type.TDate()
    case "varchar" => Type.TString()
    case "char"    => Type.TString()
    case "string"  => Type.TString()
    case "bool"    => Type.TBool()
    case "rowid"   => Type.TRowId()
    case "type"    => Type.TType()
    case _ =>  throw new SQLException("Invalid Type '" + t + "'");
  }

  def fromStringPrimitive(t: StringPrimitive) = fromString(t.asString)
}
*/

/**
 * Base type for expression trees.  Represents a single node in the tree.
 */
abstract class Expression { 
  /**
   * Return all of the children of the current tree node
   */
  def children: List[Expression] 
  /**
   * Return a new instance of the same object, but with the 
   * children replaced with the provided list.  The list must
   * be of the same size returned by children.  This is mostly
   * to facilitate recur, below
   */
  def rebuild(c: List[Expression]): Expression
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
abstract class LeafExpression extends Expression {
  def children = List[Expression]();
  def rebuild(c: List[Expression]):Expression = { return this }
}

/////////////// Primitive Values ///////////////

/**
 * Slightly more specific base type for constant terms.  PrimitiveValue
 * also acts as a boxing type for constants in Mimir.
 */
abstract class PrimitiveValue(t: Type)
  extends LeafExpression 
{
  def getType = t
  /**
   * Convert the current object into a long or throw a TypeException if 
   * not possible
   */
  def asLong: Long;
  /**
   * Convert the current object into a double or throw a TypeException if 
   * not possible
   */
  def asDouble: Double;
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
  def payload: Object;
}
/**
 * Boxed representation of a long integer
 */
case class IntPrimitive(v: Long) 
  extends PrimitiveValue(TInt())
{
  override def toString() = v.toString
  def asLong: Long = v;
  def asDouble: Double = v.toDouble;
  def asString: String = v.toString;
  def payload: Object = v.asInstanceOf[Object];
}
/*
case class UserPrimitive(v: String)
  extends PrimitiveValue(TUser)
{
  override def toString() = v.toString
  def asLong: Long = v.toLong;
  def asDouble: Double = v.toDouble;
  def asString: String = v;
  def payload: Object = v.asInstanceOf[Object];
}
*/
/**
 * Boxed representation of a string
 */
case class StringPrimitive(v: String) 
  extends PrimitiveValue(TString())
{
  override def toString() = "'"+v.toString+"'"
  def asLong: Long = java.lang.Long.parseLong(v)
  def asDouble: Double = java.lang.Double.parseDouble(v)
  def asString: String = v;
  def payload: Object = v.asInstanceOf[Object];
}
/**
 * Boxed representation of a type object
 */
case class TypePrimitive(t: Type)
  extends PrimitiveValue(TType())
{
  override def toString() = t.toString
  def asLong: Long = throw new TypeException(TType(), TInt(), "Cast")
  def asDouble: Double = throw new TypeException(TType(), TFloat(), "Cast")
  def asString: String = t.toString;
  def payload: Object = t.asInstanceOf[Object];
}
/**
 * Boxed representation of a row identifier/provenance token
 */
case class RowIdPrimitive(v: String)
  extends PrimitiveValue(TRowId())
{
  override def toString() = "'"+v.toString+"'"
  def asLong: Long = java.lang.Long.parseLong(v)
  def asDouble: Double = java.lang.Double.parseDouble(v)
  def asString: String = v;
  def payload: Object = v.asInstanceOf[Object];
}
/**
 * Boxed representation of a double-precision floating point number
 */
case class FloatPrimitive(v: Double) 
  extends PrimitiveValue(TFloat())
{
  override def toString() = v.toString
  def asLong: Long = throw new TypeException(TFloat(), TInt(), "Cast");
  def asDouble: Double = v
  def asString: String = v.toString;
  def payload: Object = v.asInstanceOf[Object];
}

/**
 * Boxed representation of a date
 */
case class DatePrimitive(y: Int, m: Int, d: Int) 
  extends PrimitiveValue(TDate())
{
  override def toString() = "DATE '"+y+"-"+m+"-"+d+"'"
  def asLong: Long = throw new TypeException(TDate(), TInt(), "Cast");
  def asDouble: Double = throw new TypeException(TDate(), TFloat(), "Cast");
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
/**
 * Boxed representation of a boolean
 */
case class BoolPrimitive(v: Boolean)
  extends PrimitiveValue(TBool())
{
  override def toString() = if(v) {"TRUE"} else {"FALSE"}
  def asLong: Long = throw new TypeException(TBool(), TInt(), "Cast");
  def asDouble: Double = throw new TypeException(TBool(), TFloat(), "Cast");
  def asString: String = toString;
  def payload: Object = v.asInstanceOf[Object];
}
/**
 * Boxed representation of NULL
 */
case class NullPrimitive()
  extends PrimitiveValue(TAny())
{
  override def toString() = "NULL"
  def asLong: Long = throw new TypeException(TAny(), TInt(), "Cast Null");
  def asDouble: Double = throw new TypeException(TAny(), TFloat(), "Cast Null");
  def asString: String = throw new TypeException(TAny(), TString(), "Cast Null");
  def payload: Object = null
}

/////////////// Computations ///////////////

/**
 * Boolean Negation
 */
case class Not(child: Expression) 
  extends Expression 
{
  def children: List[Expression] = List[Expression](child)
  def rebuild(x: List[Expression]): Expression = Not(x(0))
  override def toString = ("NOT(" + child.toString + ")")
}

/**
 * Utility class supporting binary arithmetic operations
 * 
 * Arith.Op is an Enumeration type for binary arithmetic operations
 */
object Arith extends Enumeration {
  type Op = Value
  val Add, Sub, Mult, Div, And, Or = Value
  
  /**
   * Regular expresion to match any and all binary operations
   */
  def matchRegex = """\+|-|\*|/|\||&""".r

  /**
   * Convert from the operator's string encoding to its Arith.Op rep
   */
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
  /**
   * Convert from the operator's Arith.Op representation to a string
   */
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
  /**
   * Is this binary operation a boolean operator (AND/OR)
   */
  def isBool(v: Op): Boolean = {
    v match {
      case And | Or => true
      case _ => false
    }
  }

  /**
   * Is this binary operation a numeric operator (+, -, *, /)
   */
  def isNumeric(v: Op): Boolean = !isBool(v)

}

/**
 * Enumerator for comparison types
 */
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

/**
 * Non-relational binary operations.  This includes integer and boolean
 * arithmetic.
 * 
 * See the Arith enum above for a full list of available operations.
 */
case class Arithmetic(op: Arith.Op, lhs: Expression, 
                      rhs: Expression) 
	extends Expression 
{
  override def toString() = 
	" (" + lhs.toString + Arith.opString(op) + rhs.toString + ") "
  def children = List(lhs, rhs)
  def rebuild(c: List[Expression]) = Arithmetic(op, c(0), c(1))
}

/**
 * Relational binary operations.  This includes equality/inequality,
 * ordering operators, and simple string comparators (e.g., SQL's LIKE)
 */
case class Comparison(op: Cmp.Op, lhs: Expression, 
                      rhs: Expression) 
	extends Expression 
{
  override def toString() = 
	" (" + lhs.toString + Cmp.opString(op) + rhs.toString + ") "
  def children = List(lhs, rhs)
  def rebuild(c: List[Expression]) = Comparison(op, c(0), c(1))
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
case class Function(op: String, params: List[Expression]) extends Expression {
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
  def rebuild(c: List[Expression]) = Function(op, c)
}

/**
 * Representation of a column reference (a SQL variable).  Names are all that's
 * needed.  Typechecker expects Operators to be self-contained, but not 
 * Expressions.
 */
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
case class RowIdVar() extends LeafExpression
{
  override def toString = "ROWID";
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

/**
 * Representation of a unary IS NULL
 */
case class IsNullExpression(child: Expression) extends Expression { 
  override def toString() = {child.toString+" IS NULL"}
  def children = List(child)
  def rebuild(c: List[Expression]) = IsNullExpression(c(0))
}
