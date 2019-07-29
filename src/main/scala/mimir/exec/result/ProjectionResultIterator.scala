package mimir.exec.result

import java.sql._
import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.algebra._
import mimir.util._
import mimir.exec._
import mimir.Database

class ProjectionResultIterator(
  tupleDefinition: Seq[ProjectArg],
  annotationDefinition: Seq[ProjectArg],
  inputSchema: Seq[(ID,Type)],
  source: ResultIterator,
  db: Database
)
  extends ResultIterator
  with LazyLogging
{

  //
  // Set up the schema details
  //
  private val inputSchemaLookup = inputSchema.toMap
  private val typeOf = db.typechecker.typeOf(_:Expression, inputSchemaLookup)

  val tupleSchema: Seq[(ID,Type)] = 
    tupleDefinition.map { case ProjectArg(name, expr) => (name, typeOf(expr)) }
  val annotationSchema: Seq[(ID,Type)] = 
    annotationDefinition.map { case ProjectArg(name, expr) => (name, typeOf(expr)) }

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
  private val evalScope: Map[ID,(Type, Row => PrimitiveValue)] =
    inputSchema.zipWithIndex.map { 
      case ((name, t), idx) => (name, (t, (_:Row)(idx))) 
    }.toMap
  private val eval = new EvalInlined[Row](evalScope, db)

  val columnOutputs: Seq[Row => PrimitiveValue] = 
    tupleDefinition.map { _.expression }.map { expr => 
      try {
        eval.compile(expr) 
      } catch { 
        case e:RAException => throw new RAException(s"Error while compiling $expr", None, e)
      }
    }
  val annotationOutputs: Seq[Row => PrimitiveValue] = 
    annotationDefinition.map { _.expression }.map { expr => 
      try {
        eval.compile(expr) 
      } catch { 
        case e:RAException => throw new RAException(s"Error while compiling $expr", None, e)
      }
    }
  val annotationIndexes: Map[ID,Int] =
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