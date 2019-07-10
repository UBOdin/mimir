package mimir.parser

import java.io.{Reader,File}
import scala.collection.mutable.Buffer
import org.jline.terminal.{Terminal,TerminalBuilder}
import org.jline.reader.{LineReader,LineReaderBuilder,EndOfFileException,UserInterruptException}
import com.typesafe.scalalogging.slf4j.LazyLogging
import fastparse.Parsed
import mimir.util.LineReaderInputSource

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
  private var pos = 0
  private var eof = false

  private def prompt =  if(inputBuffer.isEmpty) { headPrompt } else { restPrompt }

  private def loadInputBuffer()
  {
    if(eof){ return; }
    try {
      while(inputBuffer.size <= pos){ 
        val lineRead = input.readLine(prompt).replace("\\n", " ") 
        logger.trace(s"Got: $lineRead")
        inputBuffer += lineRead
      }
    } catch {
      case _ : EndOfFileException => eof = true;
    }
  }

  def hasNext(): Boolean =
    { pos = 0; loadInputBuffer(); !inputBuffer.isEmpty }

  def parse(): Parsed[MimirCommand] =
    fastparse.parse(inputBufferIterator:Iterator[String], MimirCommand.command(_)) 

  def next(): Parsed[MimirCommand] =
  {
    pos = 0; loadInputBuffer()
    if(!inputBuffer.isEmpty) {
      parse() match {
        case r@Parsed.Success(result, index) => 
          logger.info(s"Parsed(index = $index): $result")
          skipBytes(index)
          return r
        case f:Parsed.Failure => 
          inputBuffer.clear()
          return f
      }
    } else {
      throw new IndexOutOfBoundsException("reading from an empty iterator")
    }
  }

  def flush()
  {
    inputBuffer.clear()
  }

  def skipBytes(offset: Int)
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

  private object inputBufferIterator 
    extends Iterator[String]
  {
    def hasNext():Boolean = 
      { loadInputBuffer(); return inputBuffer.size > pos }
    def next():String = { 
      loadInputBuffer()
      if(inputBuffer.size <= pos){ 
        throw new IndexOutOfBoundsException("reading from an empty iterator")
      } else { 
        val ret = inputBuffer(pos)
        pos += 1
        return ret
      }
    }
  }
}