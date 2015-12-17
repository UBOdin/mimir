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

case class ProjectArg(column: String, input: Expression) 
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
    prefix + "UNION(" +
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