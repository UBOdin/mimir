package mimir.exec.stream

import java.sql._
import mimir.algebra._
import mimir.util._

class ProjectionResultIterator(
  tupleDefinition: Seq[ProjectArg],
  annotationDefinition: Seq[ProjectArg],
  inputSchema: Seq[(String,Type)],
  source: ResultSet
)
  extends ResultIterator
{
  //
  // Set up the schema details
  //
  private val typechecker = new ExpressionChecker(inputSchema.toMap)
  val schema: Seq[(String,(Int, Type))] = 
    tupleDefinition.zipWithIndex.map { case (ProjectArg(name, expr), idx) => (name, (idx, typechecker.typeOf(expr))) }
  val annotations: Seq[(String,(Int, Type))] = ???
    annotationDefinition.zipWithIndex.map { case (ProjectArg(name, expr), idx) => (name, (idx, typechecker.typeOf(expr))) }

  //
  // Compile the query expressions further to make it
  // faster to read data out.  For example, we can create
  // a Proc object to replace each variable, preventing
  // us from having to do value lookup by string.
  // 
  // The following few values/functions do the inlining
  // and the result is the columnOutput, annotationOutput 
  // elements defined below.
  // 
  private val inputProcs: Map[String,Proc] =
  {
    inputSchema.zipWithIndex.map { case ((name, t), idx) =>
      (name -> new Proc(Seq()) {
        def getType(argTypes: Seq[Type]): Type = t
        def get(v: Seq[PrimitiveValue]): PrimitiveValue =
          JDBCUtils.convertField(t, source, idx+1)
        def rebuild(x: Seq[Expression]) = this
      })
    }.toMap
  }  
  private def inlineProcs(expr: Expression): Expression =
  {
    expr match {
      case Var(name) => inputProcs(name)
      case _ => expr.recur(inlineProcs(_))
    }
  }
  private def compile(arg: ProjectArg): (() => PrimitiveValue) =
  {
    Eval.simplify(arg.expression) match {
      case pv: PrimitiveValue => return { () => pv }
      case expr => {
        val inlined = inlineProcs(expr)
        return { () => Eval.eval(inlined) }
      }
    }
  }

  val columnOutputs: Seq[() => PrimitiveValue] = tupleDefinition.map( compile(_) )
  val annotationOutputs: Seq[() => PrimitiveValue] = annotationDefinition.map( compile(_) )

  def close(): Unit = source.close()

  var currentRow: Option[Row] = None;

  def bufferRow()
  {
    if(currentRow == None){
      while(source.isBeforeFirst()){ if(!source.next){ return } }
      if(source.next){
        currentRow = Some(new Row(
          columnOutputs.map(_()), 
          annotationOutputs.map(_()),
          this
        ))
      }
    }
  }

  def hasNext(): Boolean = 
  {
    bufferRow()
    return currentRow != None
  }
  def next(): Row = 
  {
    bufferRow()
    currentRow match {
      case None => throw new SQLException("Trying to read past the end of an empty iterator")
      case Some(row) => {
        currentRow = None
        return row
      }
    }
  }
}