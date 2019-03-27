package mimir.parser

import fastparse.{ Parsed, parse }
import scala.collection.mutable.Queue
import java.io.{ Reader, BufferedReader, File, FileReader }

sealed abstract class EndlessParserResult
case class EndlessParserCommand(command: MimirCommand) extends EndlessParserResult
case class EndlessParserParseError(error: SyntaxError) extends EndlessParserResult
case class EndlessParserNeedMoreData() extends EndlessParserResult

class EndlessParser(
  clearOnError: Boolean = true
){

  var buffer = new Queue[String]();

  def fullBuffer = buffer.fold(""){ _ + _ }
  def charsRemaining = buffer.map { _.size }.sum

  def next(
    verbose:Boolean = false
  ): EndlessParserResult =  
    parse(
      buffer.iterator, 
      MimirCommand.command(_), 
      verboseFailures = verbose
    ) match {
      case Parsed.Success(command, idx)           => dropChars(idx); 
                                                    return EndlessParserCommand(command)
      case Parsed.Failure(msg, idx, extras) if idx >= charsRemaining 
                                                  => return EndlessParserNeedMoreData()
      case f@Parsed.Failure(_, idx, _) if verbose => return EndlessParserParseError(SyntaxError(lineOfIndex(idx), idx, f.longMsg))
      case _:Parsed.Failure                       => return next(verbose = true)
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
    var totalDropped = 0;
    while(totalDropped < n && !buffer.isEmpty){
      val stillRemainingToDrop = n - totalDropped
      if(buffer.head.length >= stillRemainingToDrop){
        totalDropped += buffer.dequeue.length
      } else {
        buffer.update(0, buffer.head.substring(stillRemainingToDrop))
        totalDropped = 0
      }
    }
  }

}