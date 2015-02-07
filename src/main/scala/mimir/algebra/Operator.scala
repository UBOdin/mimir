package mimir.algebra;

import scala.reflect.runtime.universe._

import java.sql._;

import mimir.util._;


abstract class Operator 
{ 
  def toString(prefix: String): String; 
  def children: List[Operator];
  def rebuild(c: List[Operator]): Operator;
  override def toString() = this.toString("");
  def schema: Map[String,Type.T];
}

case class ProjectArg(column: String, input: Expression) 
{
  override def toString = (column.toString + " <= " + input.toString)
}

case class Project(columns: List[ProjectArg], source: Operator) extends Operator 
{
  def toString(prefix: String) = (
    prefix + "Project[" + 
      columns.map( _.toString ).mkString(", ") +
    "]\n" + source.toString(prefix + "  ")
  );
  def children() = List(source);
  def rebuild(x: List[Operator]) = Project(columns, x(0));
  def get(v: String): Option[Expression] = 
    columns.find( (_.column == v) ).map ( _.input )
  def bindings: Map[String, Expression] = (
    columns.map( (x) => (x.column, x.input) ).toMap
  )
  def schema: Map[String,Type.T] = {
    val sch = source.schema.toMap
    // println(this.toString(""))
    // println(sch.toString)
    columns.map( (x) => (x.column, 
                         x.input.exprType(sch)) 
               ).toMap[String,Type.T]
               
  }
}

case class Select(condition: Expression, source: Operator) extends Operator
{
  def toString(prefix: String) = (
    prefix + "Select[" + condition.toString + "]\n" + source.toString(prefix+"  ")
  );
  def children() = List(source);
  def rebuild(x: List[Operator]) = Select(condition, x(0));  
  def schema: Map[String,Type.T] = source.schema;
}

case class Join(left: Operator, right: Operator) extends Operator
{
  def toString(prefix: String) = (
    // prefix + "Join of\n" + left.toString(prefix+"  ") + "\n" + prefix + "and\n" + right.toString(prefix+"  ")
    prefix + "Join\n" + left.toString(prefix+"  ") + "\n" + right.toString(prefix+"  ")
  );
  def children() = List(left, right);
  def rebuild(x: List[Operator]) = Join(x(0), x(1))
  def schema: Map[String,Type.T] = left.schema ++ right.schema
}

case class Union(left: Operator, right: Operator) extends Operator
{
  def toString(prefix: String) = (
    // prefix + "Union of\n" + left.toString(prefix+"  ") + prefix + "and\n" + right.toString(prefix+"  ")
    prefix + "Union\n" + left.toString(prefix+"  ") + "\n" + right.toString(prefix+"  ")
  );
  def children() = List(left, right);
  def rebuild(x: List[Operator]) = Union(x(0), x(1))
  def schema: Map[String,Type.T] = {
    val lsch = left.schema;
    val rsch = right.schema;
    if(lsch.keys != rsch.keys){
      throw new Exception("Schema Mismatch: ("+lsch.keys+") vs ("+rsch.keys+") in\n"+toString())
    } else {
      lsch.keys.map( (k) => 
        (k, Arith.escalateCompat(lsch.get(k).get, rsch.get(k).get))
      ).toMap
    }
  }
}

case class Table(name: String, 
                 sch: Map[String,Type.T],
                 metadata: Map[String,Type.T]) 
  extends Operator
{
  def toString(prefix: String) = ( 
    prefix + "Table("+name+" => "+(
      sch.keys.mkString(", ") + 
      ( if(metadata.size > 0)
             { " // "+metadata.keys.mkString(", ") } 
        else { "" }
      )
    )+")" 
  )
  def children: List[Operator] = List()
  def rebuild(x: List[Operator]) = Table(name, sch, metadata)
  def schema: Map[String,Type.T] = sch++metadata
}