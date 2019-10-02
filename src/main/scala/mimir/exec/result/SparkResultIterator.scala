package mimir.exec.result

import org.apache.spark.sql.DataFrame
import com.typesafe.scalalogging.LazyLogging

import mimir.Database
import mimir.algebra._
import mimir.util.SparkUtils
import mimir.util.Timer

class SparkResultIterator(
  inputSchema: Seq[(ID,Type)],
  query: Operator,
  db: Database
) 
  extends ResultIterator
  with LazyLogging 
{
  def tupleSchema: Seq[(ID, Type)] = inputSchema
  def annotationSchema: Seq[(ID, Type)] = Seq()
  
  val extractInputs: org.apache.spark.sql.Row => Seq[() => PrimitiveValue] = 
    row => inputSchema.
      zipWithIndex.
      map { case ((name, t), idx) => 
        logger.debug(s"Extracting Raw: $name (@$idx) -> $t")
        val fn = SparkUtils.convertFunction(t, idx)
        () => { fn(row) }
      }

  var closed = false
  lazy val dataframeIter = 
  {
    // Deploy to the backend
    Timer.monitor(s"EXECUTE", logger.info(_)){
      db.compiler.compileToSparkWithoutRewrites(query)
                 .cache()
                 .collect()
                 .toIterator
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