package mimir.algebra;


abstract class Operator 
{ 
  def toString(prefix: String): String; 
  def children: List[Operator];
  def rebuild(c: List[Operator]): Operator;
  override def toString() = this.toString("");
  def schema: List[(String, Type.T)] = 
    Typechecker.schemaOf(this)
}

case class ProjectArg(column: String, input: Expression)            //column is the SQL arg, input is the RA expression
{
  override def toString = (column.toString + " <= " + input.toString)
  def getColumnName() = column
}

case class Project(columns: List[ProjectArg], source: Operator) extends Operator 
{
  def toString(prefix: String) =
    prefix + "PROJECT[" + 
      columns.map( _.toString ).mkString(", ") +
    "](\n" + source.toString(prefix + "  ") + 
      "\n" + prefix + ")"

  def children() = List(source);
  def rebuild(x: List[Operator]) = Project(columns, x.head)
  def get(v: String): Option[Expression] = 
    columns.find( (_.column == v) ).map ( _.input )
  def bindings: Map[String, Expression] =
    columns.map( (x) => (x.column, x.input) ).toMap
}

/* AggregateArg is a wrapper for the args argument in Aggregate case class where:
      function is the Aggregate function,
      column is/are the SQL table column(s) parameters,
      alias is the alias used for the aggregate column output,
      getOperatorName returns the operator name,
      getColumnName returns the column name
*/

/* to fix list: first, we need to get the correct aliases; second, we need to make Aggregate have a list of AggregateArgs;
third, we need to test and then branch in SqlToRa.scala (flat or agg select)
 */
case class AggregateArg(function: String, columns: List[Expression], alias: String)
{
  override def toString = (function.toString + "(" + columns.map(_.toString).mkString(", ") + ")" + ", " + alias)
  def getFunctionName() = function
  def getColumnNames() = columns.map(x => x.toString).mkString(", ")
  def getAlias() = alias.toString
}

/* Aggregate Operator refashioned 5/23/16, 5/31/16 */
case class Aggregate(args: List[AggregateArg], groupby: List[Expression], source: Operator) extends Operator
{
  def toString(prefix: String) =
    prefix + "AGGREGATE[" + args.map(_.toString).mkString("; ") + "]\n\t(Group By [" + groupby.map( _.toString ).mkString(", ") +
      "])\n\t\t(" + source.toString(prefix + " ") + prefix + ")"

  def children() = List(source)
  def rebuild(x: List[Operator]) = new Aggregate(args, groupby, x(0))
  //def getAliases() = args.map(x => x.getAlias())
}

case class Select(condition: Expression, source: Operator) extends Operator
{
  def toString(prefix: String) =
    prefix + "SELECT[" + condition.toString + "](\n" + source.toString(prefix+"  ") + 
                  "\n" + prefix + ")"

  def children() = List(source)
  def rebuild(x: List[Operator]) = new Select(condition, x(0))
}

case class Join(left: Operator, right: Operator) extends Operator
{
  def toString(prefix: String) =
    // prefix + "Join of\n" + left.toString(prefix+"  ") + "\n" + prefix + "and\n" + right.toString(prefix+"  ")
    prefix + "JOIN(\n" + left.toString(prefix+"  ") + ",\n" + right.toString(prefix+"  ") + 
                  "\n" + prefix + ")"
  def children() = List(left, right);
  def rebuild(x: List[Operator]) = Join(x(0), x(1))
}

case class Union(left: Operator, right: Operator) extends Operator
{
  def toString(prefix: String) =
    prefix + "UNION(\n" +
        left.toString(prefix+"  ") + ",\n" + 
        right.toString(prefix+"  ") + "\n" + 
    prefix + ")";

  def children() = List(left, right)
  def rebuild(x: List[Operator]) = Union(x(0), x(1))
}

case class Table(name: String, 
                 sch: List[(String,Type.T)],
                 metadata: List[(String,Type.T)])
  extends Operator
{
  def toString(prefix: String) =
    prefix + name + "(" + (
      sch.map( { case (v,t) => v+":"+Type.toString(t) } ).mkString(", ") + 
      ( if(metadata.size > 0)
             { " // "+metadata.map( { case (v,t) => v+":"+Type.toString(t) } ).mkString(", ") } 
        else { "" }
      )
    )+")" 
  def children: List[Operator] = List()
  def rebuild(x: List[Operator]) = Table(name, sch, metadata)
}