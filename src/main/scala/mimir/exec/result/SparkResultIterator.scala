package mimir.exec.result

import org.apache.spark.sql.DataFrame
import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.algebra._
import mimir.sql.RABackend
import mimir.util.SparkUtils
import mimir.util.Timer

class SparkResultIterator(
  inputSchema: Seq[(String,Type)],
  query: Operator,
  backend: RABackend,
  dateType: (Type)
) 
  extends ResultIterator
  with LazyLogging 
{
  def tupleSchema: Seq[(String, Type)] = inputSchema
  def annotationSchema: Seq[(String, Type)] = Seq()
  
  val extractInputs: org.apache.spark.sql.Row => Seq[() => PrimitiveValue] = 
    row => inputSchema.
      zipWithIndex.
      map { case ((name, t), idx) => 
        logger.debug(s"Extracting Raw: $name (@$idx) -> $t")
        val fn = SparkUtils.convertFunction(t, idx, dateType = dateType)
        () => { fn(row) }
      }

  var closed = false
  lazy val dataframeIter = 
  {
    // Deploy to the backend
    Timer.monitor(s"EXECUTE", logger.info(_)){
      backend.execute(query).rdd.toLocalIterator 
    }
  }
  
  def close(): Unit = {
    closed = true
  }

  def hasNext(): Boolean = dataframeIter.hasNext 
  def next(): Row = { 
    ExplicitRow(extractInputs(dataframeIter.next).map { _() }, Seq(), this)
  }

}