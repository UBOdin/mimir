package mimir.algebra;

import mimir.util.ListUtils
import mimir.views.ViewAnnotation

/**
 * Abstract parent class of all relational algebra operators
 */
sealed abstract class Operator 
  extends Serializable
{ 
  /**
   * Convert the operator into a string.  Because operators are
   * nested recursively, and can span multiple lines, Every line 
   * of output should be prefixed with the specified string.
   */
  def toString(prefix: String): String; 
  /**
   * The starting point for stringification is to have no indentation
   */
  override def toString() = this.toString("");

  /**
   * Return all of the child nodes of this operator
   */
  def children: Seq[Operator];
  /**
   * Return a new instance of the same object, but with the 
   * children replaced with the provided list.  The list must
   * be of the same size returned by children.  This is mostly
   * to facilitate recur, below
   */
  def rebuild(c: Seq[Operator]): Operator;
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
  def recur(f: Operator => Operator) =
    rebuild(children.map(f))

  /**
   * Convenience method to invoke the Typechecker
   */
  def schema: Seq[(String, Type)] = Typechecker.schemaOf(this)
   

  /**
   * Convenience method to get an expression checker for this
   * schema
   */
  def typechecker: ExpressionChecker =
    Typechecker.typecheckerFor(this)

  /**
   * Convenience method to get the column names from schema
   */
  def columnNames: Seq[String] =
    schema.map(_._1)

  /**
   * Return all expression objects that appear in this node
   */
  def expressions: Seq[Expression]

  /** 
   * Replace all of the expressions in this operator.  Like 
   * rebuild, this method expects expressions to arrive in
   * the same order as they're returned by the expressions 
   * method
   */
  def rebuildExpressions(x: Seq[Expression]): Operator

  /**
   * Apply a method to recursively rewrite all of the Expressions
   * in this object.
   */
  def recurExpressions(op: Expression => Expression): Operator =
    rebuildExpressions(expressions.map( op(_) ))

  /**
   * Apply a method to recursively rewrite all of the Expressions
   * in this object, with types available
   */
  def recurExpressions(op: (Expression, ExpressionChecker) => Expression): Operator =
  {
    val checker = 
      children match {
        case Nil         => new ExpressionChecker()
        case List(child) => Typechecker.typecheckerFor(child)
        case _           => new ExpressionChecker()
    }
    recurExpressions(op(_, checker))
  }
}

/**
 * A single column output by a projection
 */
case class ProjectArg(name: String, expression: Expression) 
  extends Serializable
{
  override def toString = (name.toString + " <= " + expression.toString)
  def toBinding = (name -> expression)
}

/**
 * Generalized relational algebra projection
 */
@SerialVersionUID(100L)
case class Project(columns: Seq[ProjectArg], source: Operator) extends Operator 
{
  def toString(prefix: String) =
    prefix + "PROJECT[" + 
      columns.map( _.toString ).mkString(", ") +
    "](\n" + source.toString(prefix + "  ") + 
      "\n" + prefix + ")"

  def children() = List(source);
  def rebuild(x: Seq[Operator]) = Project(columns, x.head)
  def get(v: String): Option[Expression] = 
    columns.find( (_.name == v) ).map ( _.expression )
  def bindings: Map[String, Expression] =
    columns.map( _.toBinding ).toMap
  def expressions = columns.map(_.expression)
  def rebuildExpressions(x: Seq[Expression]) = Project(
    columns.zip(x).map({ case (ProjectArg(name,_),expr) => ProjectArg(name, expr)}),
    source
  )
}

/* AggregateArg is a wrapper for the args argument in Aggregate case class where:
      function is the Aggregate function,
      column is/are the SQL table column(s) parameters,
      alias is the alias used for the aggregate column output,
      getOperatorName returns the operator name,
      getColumnName returns the column name
*/
@SerialVersionUID(100L)
case class AggFunction(function: String, distinct: Boolean, args: Seq[Expression], alias: String)
  extends Serializable
{
  override def toString = (alias + " <= " + function.toString + "(" + (if(distinct){"DISTINCT "}else{""}) + args.map(_.toString).mkString(", ") + ")")
  def getFunctionName() = function
  def getColumnNames() = args.map(x => x.toString).mkString(", ")
  def getAlias() = alias.toString
}

@SerialVersionUID(100L)
case class Aggregate(groupby: Seq[Var], aggregates: Seq[AggFunction], source: Operator) extends Operator
{
  def toString(prefix: String) =
    prefix + "AGGREGATE[" + 
      (groupby ++ aggregates).mkString(", ") + 
      "](\n" +
      source.toString(prefix + "  ") + "\n" + prefix + ")"

  def children() = List(source)
  def rebuild(x: Seq[Operator]) = new Aggregate(groupby, aggregates, x(0))
  def expressions = groupby ++ aggregates.flatMap(_.args)
  def rebuildExpressions(x: Seq[Expression]) = {
    val remainingExpressions = x.iterator
    val newGroupBy = 
      ListUtils.headN(remainingExpressions, groupby.length).
        map(_.asInstanceOf[Var])

    val newAggregates = 
      aggregates.
        map(curr => {
          val newArgs = 
            ListUtils.headN(remainingExpressions, curr.args.size)
          AggFunction(curr.function, curr.distinct, newArgs, curr.alias)
        })
    Aggregate(newGroupBy, newAggregates, source)
  }
}

/**
 * Relational algebra selection
 */
@SerialVersionUID(100L)
case class Select(condition: Expression, source: Operator) extends Operator
{
  def toString(prefix: String) =
    prefix + "SELECT[" + condition.toString + "](\n" + source.toString(prefix+"  ") + 
                  "\n" + prefix + ")"

  def children() = List(source)
  def rebuild(x: Seq[Operator]) = new Select(condition, x(0))
  def expressions = List(condition)
  def rebuildExpressions(x: Seq[Expression]) = Select(x(0), source)
}


/**
 * invisify provenance attributes operator -- With Provenance
 */
case class Annotate(subj: Operator,
                 invisSch: Seq[(ProjectArg, (String,Type), String)])
  extends Operator
{
  def toString(prefix: String) =
    prefix + "ANNOTATE(" + 
      ("\n" + subj.toString(prefix+"  ") +"\n" + prefix 
      )+")" + 
       ( if(invisSch.size > 0)
             { " // "+invisSch.map( { case (c,(v,t), ta) => c + ":"+t } ).mkString(", ") }
        else { "" })
  def children: List[Operator] = List(subj)
  def rebuild(x: Seq[Operator]) = Annotate(x.head, invisSch)
  def invisible_schema = invisSch.map( x => (x._1, x._2) )
  def expressions = invisSch.map(invsCol => invsCol._1.expression )
  def rebuildExpressions(x: Seq[Expression]) = Annotate(subj,
    invisSch.zip(x).map({ case ((ProjectArg(name,_),ntt,alias),expr) => (ProjectArg(name,expr),ntt,alias)})   
  )
}

/**
 * visify provenance attributes operator -- Provenance Of
 */
case class Recover(subj: Operator,
                 invisSch: Seq[(ProjectArg, (String,Type), String)]) extends Operator
{
  def toString(prefix: String) =
    // prefix + "Join of\n" + left.toString(prefix+"  ") + "\n" + prefix + "and\n" + right.toString(prefix+"  ")
    prefix + "RECOVER(\n" + subj.toString(prefix+"  ") + 
                  "\n" + prefix + ")" + 
       ( if(invisSch.size > 0)
             { " // "+invisSch.map( { case (c,(v,t), ta) => c+":"+t } ).mkString(", ") }
        else { "" })
  def children() = List(subj);
  def rebuild(x: Seq[Operator]) = Recover(x.head, invisSch)
  def expressions = invisSch.map(invsCol => invsCol._1.expression )
  def rebuildExpressions(x: Seq[Expression]) = Recover(subj,
    invisSch.zip(x).map({ case ((ProjectArg(name,_),ntt,alias),expr) => (ProjectArg(name,expr),ntt,alias)})   
  )
}

/**
 * provenance of operator -- Provenance Of
 */
case class ProvenanceOf(subj: Operator) extends Operator
{
  def toString(prefix: String) =
    // prefix + "Join of\n" + left.toString(prefix+"  ") + "\n" + prefix + "and\n" + right.toString(prefix+"  ")
    prefix + "PROVENANCE(\n" + subj.toString(prefix+"  ") + 
                  "\n" + prefix + ")"
  def children() = List(subj);
  def rebuild(x: Seq[Operator]) = ProvenanceOf(x(0))
  def expressions = List()
  def rebuildExpressions(x: Seq[Expression]) = this
}

/**
 * Relational algebra cartesian product (I know, technically not an actual join)
 */
@SerialVersionUID(100L)
case class Join(left: Operator, right: Operator) extends Operator
{
  def toString(prefix: String) =
    // prefix + "Join of\n" + left.toString(prefix+"  ") + "\n" + prefix + "and\n" + right.toString(prefix+"  ")
    prefix + "JOIN(\n" + left.toString(prefix+"  ") + ",\n" + right.toString(prefix+"  ") + 
                  "\n" + prefix + ")"
  def children() = List(left, right);
  def rebuild(x: Seq[Operator]) = Join(x(0), x(1))
  def expressions = List()
  def rebuildExpressions(x: Seq[Expression]) = this
}

/**
 * Relational algebra bag union
 */
@SerialVersionUID(100L)
case class Union(left: Operator, right: Operator) extends Operator
{
  def toString(prefix: String) =
    prefix + "UNION(\n" +
        left.toString(prefix+"  ") + ",\n" + 
        right.toString(prefix+"  ") + "\n" + 
    prefix + ")";

  def children() = List(left, right)
  def rebuild(x: Seq[Operator]) = Union(x(0), x(1))
  def expressions = List()
  def rebuildExpressions(x: Seq[Expression]) = this
}

/**
 * A base relation (Table).
 *
 * Note that schema information is required to make Typechecking self-contained 
 * and database-independent
 *
 * Metadata columns are special implicit attributes used by many database 
 * backends.  They are specified as follows:
 *   (output, input, type)
 * Where:
 *   output: The name that the implicit attribute will be referenced by in the 
 *           output relation
 *   input:  An expression to extract the implicit attribute
 *   type:   The type of the implicit attribute.
 * For example: 
 *   ("MIMIR_ROWID", Var("ROWID"), Type.TRowId())
 * will extract SQL's implicit ROWID attribute into the new column "MIMIR_ROWID" 
 * with the rowid type.
 */
@SerialVersionUID(100L)
case class Table(name: String, 
                 alias: String, 
                 sch: Seq[(String,Type)],
                 metadata: Seq[(String,Expression,Type)])
  extends Operator
{
  def toString(prefix: String) =
    prefix + name + "(" + (
      sch.map( { case (v,t) => v+":"+t } ).mkString(", ") +
      ( if(metadata.size > 0)
             { " // "+metadata.map( { case (v,e,t) => v+":"+t+" <- "+e } ).mkString(", ") }
        else { "" }
      )
    )+")" 
  def children: List[Operator] = List()
  def rebuild(x: Seq[Operator]) = Table(name, alias, sch, metadata)
  def metadata_schema = metadata.map( x => (x._1, x._3) )
  def expressions = List()
  def rebuildExpressions(x: Seq[Expression]) = this
}

/**
 * A blank table --- Corresponds roughly to Oracle's DUAL, or a 
 * SELECT ... FROM ... WHERE FALSE.
 * 
 * Not really used, just a placeholder for intermediate optimization.
 */
@SerialVersionUID(100L)
case class EmptyTable(sch: Seq[(String, Type)])
  extends Operator
{
    def toString(prefix: String) =
    prefix + "!!EMPTY!!(" + (
      sch.map( { case (v,t) => v+":"+t } ).mkString(", ") 
    )+")" 
  def children: List[Operator] = List()
  def rebuild(x: Seq[Operator]) = this
  def expressions = List()
  def rebuildExpressions(x: Seq[Expression]) = this
}

/**
 * A single sort directive
 *
 * Consists of a column name, as well as a binary "ascending" 
 * or "descending"
 *
 * (e.g., as in SELECT * FROM FOO ORDER BY bar ASC)
 */
@SerialVersionUID(100L)
case class SortColumn(expression: Expression, ascending:Boolean)
{
  override def toString() = 
    (expression + " " + (if(ascending){"ASC"}else{"DESC"}))
}
/**
 * Indicates that the source operator's output should be sorted in the
 * specified order
 */
@SerialVersionUID(100L)
case class Sort(sorts:Seq[SortColumn], src: Operator) extends Operator
{
  def toString(prefix: String) =
  {
    prefix + "SORT[" + 
        sorts.map(_.toString).mkString(", ") +
      "](\n" +
        src.toString(prefix+"  ") +
      "\n"+prefix+")"
  }
  def children: List[Operator] = List(src)
  def rebuild(x: Seq[Operator]) = Sort(sorts, x(0))
  def expressions = sorts.map(_.expression)
  def rebuildExpressions(x: Seq[Expression]) = 
    Sort(x.zip(sorts).map { col => SortColumn(col._1, col._2.ascending) }, src)
}

/**
 * Indicates that the source operator's output should be truncated
 * after the specified number of rows.
 */
@SerialVersionUID(100L)
case class Limit(offset: Long, count: Option[Long], src: Operator) extends Operator
{
  def toString(prefix: String) =
  {
    prefix + "LIMIT["+offset+","+
        (count match { case None => "X"; case Some(i) => i })+
      "](\n" +
        src.toString(prefix+"  ") +
      ")"
  }
  def children: List[Operator] = List(src)
  def rebuild(x: Seq[Operator]) = Limit(offset, count, x(0))
  def expressions = List()
  def rebuildExpressions(x: Seq[Expression]) = this
}

/**
 * A left outer join
 */
@SerialVersionUID(100L)
case class LeftOuterJoin(left: Operator, 
                         right: Operator,
                         condition: Expression)
  extends Operator
{
  def toString(prefix: String): String =
    prefix+"LEFTOUTERJOIN("+condition+",\n"+
      left.toString(prefix+"   ")+",\n"+
      right.toString(prefix+"   ")+"\n"+
    prefix+")"

  def children: List[Operator] = 
    List(left, right)
  def rebuild(c: Seq[Operator]): Operator =
    LeftOuterJoin(c(0), c(1), condition)
  def expressions: List[Expression] = List(condition)
  def rebuildExpressions(x: Seq[Expression]) = LeftOuterJoin(left, right, x(0))
}

/**
 * A materialized view
 *
 * When initialized by RAToSql, the query field will contain the raw unmodified 
 * query that the view was instantiated with.  As the view goes through compilation,
 * the nested query will be modified; The metadata field tracks which forms of 
 * compilation have been applied to it, so that the system can decide whether it has
 * an appropriate materialized form of the view ready.
 */
@SerialVersionUID(100L)
case class View(name: String, query: Operator, annotations: Set[ViewAnnotation.T] = Set())
  extends Operator
{
  def children: Seq[Operator] = Seq(query)
  def expressions: Seq[Expression] = Seq()
  def rebuild(c: Seq[Operator]): Operator = View(name, c(0), annotations)
  def rebuildExpressions(x: Seq[Expression]): Operator = this
  def toString(prefix: String): String = 
    s"$prefix$name[${annotations.mkString(", ")}] := (\n${query.toString(prefix+"   ")}\n$prefix)"
}