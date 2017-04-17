package mimir.algebra;

/**
 * Base type for expression trees.  Represents a single node in the tree.
 */
abstract class Expression { 
  /**
   * Return all of the children of the current tree node
   */
  def children: Seq[Expression] 
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
abstract class LeafExpression extends Expression {
  def children = List[Expression]();
  def rebuild(c: Seq[Expression]):Expression = { return this }
}

/////////////// Computations ///////////////

/**
 * Boolean Negation
 */
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
  def opString(v: Op): String = {
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
  def isBool(v: Op): Boolean = {
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
  def rebuild(c: Seq[Expression]) = Arithmetic(op, c(0), c(1))
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
 * Representation of a JDBC Variable.
 */
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
case class IsNullExpression(child: Expression) extends Expression { 
  override def toString() = {child.toString+" IS NULL"}
  def children = List(child)
  def rebuild(c: Seq[Expression]) = IsNullExpression(c(0))
}
