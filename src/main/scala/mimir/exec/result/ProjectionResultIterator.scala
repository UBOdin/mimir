package mimir.exec.result

import java.sql._
import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.algebra._
import mimir.util._
import mimir.exec._

class ProjectionResultIterator(
  tupleDefinition: Seq[ProjectArg],
  annotationDefinition: Seq[ProjectArg],
  inputSchema: Seq[(String,Type)],
  source: ResultIterator
)
  extends ResultIterator
  with LazyLogging
{

  //
  // Set up the schema details
  //
  private val typechecker = new ExpressionChecker(inputSchema.toMap)
  val tupleSchema: Seq[(String,Type)] = 
    tupleDefinition.map { case ProjectArg(name, expr) => (name, typechecker.typeOf(expr)) }
  val annotationSchema: Seq[(String,Type)] = 
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
  private val evalScope: Map[String,(Type, Row => PrimitiveValue)] =
    inputSchema.zipWithIndex.map { 
      case ((name, t), idx) => (name, (t, (_:Row)(idx))) 
    }.toMap
  private val eval = new EvalInlined[Row](evalScope)

  val columnOutputs: Seq[Row => PrimitiveValue] = 
    tupleDefinition.map { _.expression }.map { eval.compile(_) }
  val annotationOutputs: Seq[Row => PrimitiveValue] = 
    annotationDefinition.map { _.expression }.map { eval.compile(_) }
  val annotationIndexes: Map[String,Int] =
    annotationDefinition.map { _.name }.zipWithIndex.toMap

  val makeRow =
    if(ExperimentalOptions.isEnabled("AGGRESSIVE-ROW-COMPUTE")) {
      (tuple: Row) => {
        new ExplicitRow(
          columnOutputs.map(_(tuple)), 
          annotationOutputs.map(_(tuple)),
          this
        )
      }
    } else {
      (tuple: Row) => {
        new LazyRow(
          tuple,
          columnOutputs,
          annotationOutputs,
          schema,
          annotationIndexes
        )
      }
    }

  def close(): Unit =
    source.close
  def hasNext(): Boolean = 
    source.hasNext
  def next(): Row = 
    makeRow(source.next())

}