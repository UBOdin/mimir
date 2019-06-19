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
    dropLeadingWhitespace
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
        val (line, lineIdx, lineNo) = lineOfIndex(idx)
        if(clearOnError) { clear() }
        return EndlessParserParseError(SyntaxError(line, lineIdx, lineNo, f.longMsg))
      }
      case _:Parsed.Failure => { 
        return next(verbose = true)
      }
    }
  }

  def iterator = new EndlessParserIterator(this)

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
      else { load(line+"\n") }
    }
  }
  def load(file: File) { load(new FileReader(file)) }

  def clear() { buffer.clear() }

  val TrailingWhitespace = "\\s+$".r.unanchored

  private def lineOfIndex(n: Int): (String, Int, Int) =
  {
    var totalCharsSeen = 0
    var lineNo = 0
    logger.trace(s"Looking for char $n")
    for(line <- buffer){
      logger.trace(s"... at char $totalCharsSeen -> $line")
      if(totalCharsSeen + line.length > n) { 
        return (
          TrailingWhitespace.replaceFirstIn(line, ""), 
          n-totalCharsSeen, 
          lineNo
        ); }
      totalCharsSeen += line.length
      lineNo += 1
    }
    return ("", 0, lineNo)
  }

  val LeadingWhitespace = "^\\s+".r.unanchored

  private def dropLeadingWhitespace
  {
    var done = false
    while(!buffer.isEmpty && !done){ 
      buffer.update(0, 
        LeadingWhitespace.replaceFirstIn(buffer.head, "")
      )
      if(!buffer.head.equals("")){ done = true }
      while(!buffer.isEmpty && buffer.head.equals("")){
        buffer.dequeue
      }
    }
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

class EndlessParserIterator(parser: EndlessParser)
  extends Iterator[MimirCommand]
{
  var buffer: Option[MimirCommand] = None

  def load: Boolean =
  {
    buffer match {
      case None => parser.next() match {
        case EndlessParserCommand(cmd)      => buffer = Some(cmd); return true
        case _:EndlessParserNeedMoreData    => return false
        case EndlessParserParseError(error) => throw new Exception(error.toString)
      }
      case Some(_) => return true
    }
  }

  def hasNext() = load
  def next(): MimirCommand = { 
    load; 
    val ret = buffer.getOrElse(null); 
    buffer = None; 
    return ret 
  }

}