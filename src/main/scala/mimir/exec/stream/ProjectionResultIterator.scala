package mimir.exec.stream

import java.sql._
import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.algebra._
import mimir.util._
import mimir.exec._

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
  private val evalScope: Map[String,(Type, Seq[PrimitiveValue] => PrimitiveValue)] =
    inputSchema.zipWithIndex.map { 
      case ((name, t), idx) => (name, (t, (_:Seq[PrimitiveValue])(idx))) 
    }.toMap
  private val eval = new EvalInlined[Seq[PrimitiveValue]](evalScope)

  val columnOutputs: Seq[Seq[PrimitiveValue] => PrimitiveValue] = 
    tupleDefinition.map { _.expression }.map { eval.compile(_) }
  val annotationOutputs: Seq[Seq[PrimitiveValue] => PrimitiveValue] = 
    annotationDefinition.map { _.expression }.map { eval.compile(_) }
  val extractInputs: Seq[() => PrimitiveValue] = 
    inputSchema.
      zipWithIndex.
      map { case ((name, t), idx) => 
        logger.debug(s"Extracting Raw: $name (@$idx) -> $t")
        val fn = JDBCUtils.convertFunction(t, idx+1, rowIdType = rowIdAndDateType._1, dateType = rowIdAndDateType._2)
        () => { fn(source) }
      }
  val annotationIndexes: Map[String,Int] =
    annotationDefinition.map { _.name }.zipWithIndex.toMap

  var closed = false
  def close(): Unit = {
    closed = true;
    source.close()
  }

  var currentRow: Option[Row] = None;

  val makeRow =
    if(ExperimentalOptions.isEnabled("AGGRESSIVE-ROW-COMPUTE")) {
      () => {
        val tuple = extractInputs.map { _() }
        new ExplicitRow(
          columnOutputs.map(_(tuple)), 
          annotationOutputs.map(_(tuple)),
          this
        )
      }
    } else {
      () => {
        new LazyRow(
          extractInputs.map { _() },
          columnOutputs,
          annotationOutputs,
          schema,
          annotationIndexes
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