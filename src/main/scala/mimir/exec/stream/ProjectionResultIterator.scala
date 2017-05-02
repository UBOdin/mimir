package mimir.exec.stream

import java.sql._
import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.algebra._
import mimir.util._

class ProjectionResultIterator(
  tupleDefinition: Seq[ProjectArg],
  annotationDefinition: Seq[ProjectArg],
  inputSchema: Seq[(String,Type)],
  source: ResultSet,
  rowIdAndDateType: (Type, Type)
)
  extends ResultIterator
  with LazyLogging
{

  //
  // Set up the schema details
  //
  private val typechecker = new ExpressionChecker(inputSchema.toMap)
  val schema: Seq[(String,Type)] = 
    tupleDefinition.map { case ProjectArg(name, expr) => (name, typechecker.typeOf(expr)) }
  val annotations: Seq[(String,Type)] = 
    annotationDefinition.map { case ProjectArg(name, expr) => (name, typechecker.typeOf(expr)) }

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
      case pv: PrimitiveValue => {
        return { () => pv }
      }
      case Var(name) => {
        val idx = inputSchema.indexWhere(_._1.equals(name))
        return { () => JDBCUtils.convertField(inputSchema(idx)._2, source, idx+1) }
      }
      case expr => {
        val inlined = inlineProcs(expr)
        return { () => Eval.eval(inlined) }
      }
    }
  }

  val columnOutputs: Seq[() => PrimitiveValue] = tupleDefinition.map( compile(_) )
  val annotationOutputs: Seq[() => PrimitiveValue] = annotationDefinition.map( compile(_) )
  val extractInputs: Seq[() => (String, PrimitiveValue)] =
    inputSchema.
      zipWithIndex.
      map { case ((name, t), idx) => 
        logger.debug(s"Extracting: $name (@$idx) -> $t")
        val fn = JDBCUtils.convertFunction(t, idx+1, rowIdType = rowIdAndDateType._1, dateType = rowIdAndDateType._2)
        () => {
          (name, fn(source))
        }
      }

  var closed = false
  def close(): Unit = {
    closed = true;
    source.close()
  }

  var currentRow: Option[Row] = None;


  val makeRow =
    if(ExperimentalOptions.isEnabled("AGGRESSIVE-ROW-COMPUTE")) {
      () => {
        new ExplicitRow(
          columnOutputs.map(_()), 
          annotationOutputs.map(_()),
          this
        )
      }
    } else {
      () => {
        new LazyRow(
          extractInputs.map { x => x() }.toMap,
          tupleDefinition,
          annotationDefinition,
          schema
        )
      }
    }

  def bufferRow()
  {
    if(closed){
      throw new SQLException("Attempting to read from closed iterator.  This is probably because you're trying to return an unmaterialized iterator from Database.query")
    }
    if(currentRow == None){
      logger.trace("BUFFERING")
      if(source.next){
        currentRow = Some(makeRow())
        logger.trace(s"READ: ${currentRow.get}")
      } else { 
        logger.trace("EMPTY")
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