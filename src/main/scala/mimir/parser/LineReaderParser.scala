package mimir.parser

import java.io.{Reader,File}
import scala.collection.mutable.Buffer
import org.jline.terminal.{Terminal,TerminalBuilder}
import org.jline.reader.{LineReader,LineReaderBuilder,EndOfFileException,UserInterruptException}
import com.typesafe.scalalogging.slf4j.LazyLogging
import fastparse.Parsed
import mimir.util.LineReaderInputSource

/**
 * Intermediary between JLine and FastParse
 */
class LineReaderParser(
  terminal: Terminal, 
  historyFile: String = LineReaderInputSource.defaultHistoryFile,
  headPrompt: String = "mimir> ",
  restPrompt: String = "     > "
)
  extends Iterator[Parsed[MimirCommand]]
  with LazyLogging
{

  private val input: LineReader = 
    LineReaderBuilder.
      builder().
      terminal(terminal).
      variable(LineReader.HISTORY_FILE, historyFile).
      build()
  private val inputBuffer = Buffer[String]()
  private var commandBuffer: Option[Parsed[MimirCommand]] = None
  // private var pos = 0
  private var eof = false

  private def prompt =  if(inputBuffer.isEmpty) { headPrompt } else { restPrompt }

  /**
   * Read a line from JLine into the input buffer
   */
  private def readLine()
  {
    while(!eof){ 
      try {
        val lineRead = input.readLine(prompt)//.replace("\\n", " ") 
        logger.trace(s"Got: $lineRead")
        if( ! lineRead.trim().equals("") ){ 
          // ignore blank lines
          inputBuffer += lineRead
          return
        }
      } catch {
        // if there's anything in the input buffer clear it and reset.  Otherwise
        // pass the exception out.
        case _ : UserInterruptException if !inputBuffer.isEmpty => inputBuffer.clear()
        case _ : EndOfFileException => eof = true;
      }
    }
  }

  /**
   * Attempt to parse the current input buffer 
   * @return    A parse result (on success or failure) or None if more data is needed
   * 
   * This implementation is a nasty hack built as a result of butting up against
   * a FastParse limitation.  FastParse gives no indication that a failure
   * is a result of an EOF or a legitimate parsing glitch.  As a result, we need
   * to get Clever about how to detect the EOF.  
   *
   * inputBufferIterator mimics inputBuffer.iterator, but includes an 
   * additional *two* "sentinel" lines at the end consisting of nothing but
   * whitespace.  If *both* lines are consumed by the parser, we take that
   * to mean that more data might change the result.  If at least one of
   * the sentinels survives, we treat it as a legitimate failure.
   */
  private def tryParse(): Option[Parsed[MimirCommand]] =
  {
    logger.trace(s"Trying to parse (${inputBuffer.size} lines)")
    inputBufferIterator.reset()
    fastparse.parse(
      inputBufferIterator,
      MimirCommand.command(_),
      verboseFailures = true
    ) match { 
      case r@Parsed.Success(result, index) => Some(r)
      case f:Parsed.Failure if inputBufferIterator.hasNext => Some(f)
      case f:Parsed.Failure => logger.trace(s"Need more input ($f)"); None
    }
  }

  /**
   * Buffer the next parser response from JLine
   * @return    A parsed command, parser error, or None if the stream is over
   * 
   * If a parser response has already been buffered, this function 
   * returns immediately.  Otherwise, lines will be read from JLine
   * until either a legitimate (i.e., non-EOF) parser failure occurs, or
   * the parser gets a command.
   *
   * A legitimate parser failure will cause the input to be flushed.
   */
  private def tryReadNext(): Option[Parsed[MimirCommand]] =
  {
    while(!eof && commandBuffer == None){
      readLine()
      if(!inputBuffer.isEmpty) {
        commandBuffer = tryParse()
        commandBuffer match {
          case Some(r@Parsed.Success(result, index)) => {
            logger.info(s"Parsed(index = $index): $result")
            skipBytes(index)
          }
          case Some(f@Parsed.Failure(token, index, extra)) => {
            inputBuffer.clear()
          }
          case None => {}
        }
      }
    }
    return commandBuffer
  }

  /**
   * Reset the state of the parser to pristine
   */
  def flush() { inputBuffer.clear(); commandBuffer = None }

  /**
   * Return true if the iterator has another parser response.
   *
   * This method blocks until a parser response becomes available or 
   * the terminal session ends.
   */
  def hasNext(): Boolean = { tryReadNext() != None }

  /**
   * Return the next parser response.
   *
   * This method blocks until a parser response becomes available or 
   * the terminal session ends.
   */
  def next(): Parsed[MimirCommand] = 
  {
    tryReadNext() match { 
      case None => 
        throw new IndexOutOfBoundsException("reading from an empty iterator")
      case Some(command) => {
        commandBuffer = None
        return command
      }
    }
  }

  /**
   * Advance the inputBuffer by a fixed number of bytes (e.g., after a successful parse).
   */
  private def skipBytes(offset: Int)
  {
    var dropped = 0
    while(offset > dropped && !inputBuffer.isEmpty){
      logger.debug(s"Checking for drop: $dropped / $offset: Next unit: ${inputBuffer.head.length}")
      if(inputBuffer.head.length < (offset - dropped)){ 
        dropped = dropped + inputBuffer.head.length
        logger.debug(s"Dropping '${inputBuffer.head}' ($dropped / $offset dropped so far)")
        inputBuffer.remove(0)
      } else {
        logger.debug(s"Trimming '${inputBuffer.head}' (by ${offset - dropped}) and done")
        var firstLine = inputBuffer.head
        // trim off the remaining characters
        firstLine = firstLine.substring(offset - dropped)
        // trim off any leading whitespace
        firstLine = firstLine.replaceFirst("^\\s+", "")

        // if that polished off all of the remaining characters, 
        // just drop the first line.
        if(firstLine.equals("")){ inputBuffer.remove(0) }

        // otherwise replace the string
        else { inputBuffer.update(0, firstLine) }

        return
      }
    }
  }

  /**
   * An iterator for InputBuffer + two extra sentinel lines
   *
   * See tryParse() above.  The short of it is that this 
   * iterator produces the same result as inputBuffer.iterator
   * but with two extra trailing "sentinel" lines of whitespace.
   */
  private object inputBufferIterator 
    extends Iterator[String]
  {
    var pos = 0

    def reset() { pos = 0 }
    def hasNext():Boolean = 
      { pos < inputBuffer.size + 2 }
    def next():String = { 
      if(pos < inputBuffer.size){
        // Normal line
        val ret = inputBuffer(pos)
        pos += 1
        return ret        
      } else {
        // Sentinel line
        pos += 1
        // need to make sure that each sentinel line is bigger 
        // the fastparse read buffer (about 10 characters by
        // default), or else both sentinel lines could be read in
        // as part of a normal error.
        return "                      "
      }
    }
  }
}