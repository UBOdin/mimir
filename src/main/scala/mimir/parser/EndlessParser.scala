package mimir.parser

import fastparse.{ Parsed, parse }
import scala.collection.mutable.Queue
import java.io.{ Reader, BufferedReader, File, FileReader }
import com.typesafe.scalalogging.slf4j.LazyLogging

sealed abstract class EndlessParserResult
case class EndlessParserCommand(command: MimirCommand) extends EndlessParserResult
case class EndlessParserParseError(error: SyntaxError) extends EndlessParserResult
case class EndlessParserNeedMoreData() extends EndlessParserResult

class EndlessParser(
  clearOnError: Boolean = true
) 
  extends LazyLogging
{

  var buffer = new Queue[String]();

  def fullBuffer = buffer.fold(""){ _ + _ }
  def charsRemaining = buffer.map { _.size }.sum
  def isEmpty = buffer.isEmpty

  def next(
    verbose:Boolean = false
  ): EndlessParserResult =  {
    logger.trace { s"Parsing Buffer: $fullBuffer" } 
    parse(
      buffer.iterator, 
      MimirCommand.command(_), 
      verboseFailures = verbose
    ) match {
      case Parsed.Success(command, idx) => { 
        dropChars(idx); 
        return EndlessParserCommand(command)
      }
      case Parsed.Failure(msg, idx, extras) if (idx >= charsRemaining) => {
        return EndlessParserNeedMoreData()
      }
      case f@Parsed.Failure(_, idx, _) if verbose => {
        if(clearOnError) { clear() }
        return EndlessParserParseError(SyntaxError(lineOfIndex(idx), idx, f.longMsg))
      }
      case _:Parsed.Failure => { 
        return next(verbose = true)
      }
    }
  }

  def load(line: String) { buffer.enqueue(line) }
  def load(source: Reader) 
  { 
    val bufferedSource = source match {
      case s: BufferedReader => s
      case _ => new BufferedReader(source)
    }
    var notDone = true
    while( notDone ){
      val line = bufferedSource.readLine()
      if(line == null){ notDone = false }
      else { load(line) }
    }
  }
  def load(file: File) { load(new FileReader(file)) }

  def clear() { buffer.clear() }

  private def lineOfIndex(n: Int): String =
  {
    var totalCharsSeen = 0
    for(line <- buffer){
      totalCharsSeen += line.length
      if(totalCharsSeen > n) { return line; }
    }
    return ""
  }

  private def dropChars(n: Int)
  {
    logger.trace(s"Dropping $n chars")
    var totalDropped = 0;
    while(totalDropped < n && !buffer.isEmpty){
      logger.trace(s"$n dropped; next string: ${buffer.head}; length = ${buffer.head.length}")
      val stillRemainingToDrop = n - totalDropped
      if(buffer.head.length <= stillRemainingToDrop){
        logger.trace("Dropping head")
        totalDropped += buffer.dequeue.length
      } else {
        buffer.update(0, buffer.head.substring(stillRemainingToDrop))
        logger.trace("Trimming head: New Length = ${buffer.head.length}")
        return
      }
    }
    logger.trace(s"Done dropping")
  }

}