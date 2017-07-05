package mimir.exec.result

import java.sql._
import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.algebra._
import mimir.util._
import mimir.exec._
import mimir.sql.Backend
import net.sf.jsqlparser.statement.select.SelectBody

class JDBCResultIterator(
  inputSchema: Seq[(String,Type)],
  query: SelectBody,
  backend: Backend,
  dateType: (Type)
)
  extends ResultIterator
  with LazyLogging
{

  def annotationSchema: Seq[(String, Type)] = Seq()
  def tupleSchema: Seq[(String, Type)] = inputSchema

  val extractInputs: Seq[() => PrimitiveValue] = 
    inputSchema.
      zipWithIndex.
      map { case ((name, t), idx) => 
        logger.debug(s"Extracting Raw: $name (@$idx) -> $t")
        val fn = JDBCUtils.convertFunction(t, idx+1, dateType = dateType)
        () => { fn(source) }
      }

  lazy val source = 
  {
    // Deploy to the backend
    TimeUtils.monitor(s"EXECUTE", logger.info(_)){
      backend.execute(query)
    }
  }

  var closed = false
  def close(): Unit = {
    closed = true;
    source.close()
  }

  var currentRow: Option[Row] = None;

  def bufferRow()
  {
    if(closed){
      throw new SQLException("Attempting to read from closed iterator.  This is probably because you're trying to return an unmaterialized iterator from Database.query")
    }
    if(currentRow == None){
      logger.trace("BUFFERING")
      if(source.next){ 
        currentRow = Some(
          ExplicitRow(extractInputs.map { _() }, Seq(), this)
        )
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