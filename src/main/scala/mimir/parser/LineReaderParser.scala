package mimir.util

import java.io.{Reader,File}
import org.jline.terminal.{Terminal,TerminalBuilder}
import org.jline.reader.{LineReader,LineReaderBuilder,EndOfFileException,UserInterruptException}
import com.typesafe.scalalogging.slf4j.LazyLogging

class LineReaderParser(
  terminal: Terminal, 
  historyFile: String = LineReaderInputSource.defaultHistoryFile,
  headPrompt: String = "mimir> ",
  restPrompt: String = "     > "
)
  extends LazyLogging
{
  val input: LineReader = 
    LineReaderBuilder.
      builder().
      terminal(terminal).
      variable(LineReader.HISTORY_FILE, historyFile).
      build()

  val buffer = Buffer[String]()


  def load()
  {
    val prompt = if(buffer.isEmpty) { headPrompt } else { restPrompt }
    buffer += input.readLine(prompt).replace("\\n", " ")
  }

  def next(): Parsed[MimirCommand] =
  {
    while(true){ 
      parser(buffer.iterator) match {
        case r@Parsed.Success(result, index) => 
          logger.info(s"Parsed(index = $index): $result")
          skipBytes(index)
          return r
        case f:Parsed.Failure => 
          buffer.clear()
          return f
      }

    }
  }
          curr = input.readLine("mimir> ")


  {

  }


  var pos: Int = 1;
  var curr: String = "";

  def close() = input.getTerminal.close
  def read(cbuf: Array[Char], offset: Int, len: Int): Int =
  {
    try { 
      var i:Int = 0;
      logger.debug(s"being asked for $len characters")
      while(i < len){
        while(pos >= curr.length){
          if(i > 0){ logger.debug(s"returning $i characters"); return i; }
          curr = input.readLine("mimir> ")
          if(curr == null){ logger.debug("Reached end"); return -1; }
          logger.debug(s"Read: '$curr'")
          pos = 0;
        }
        cbuf(i+offset) = curr.charAt(pos);
        i += 1; pos += 1;
      }
      logger.debug(s"Full!  Returning $i characters")
      return i;
    } catch {
      case _ : EndOfFileException => return -1;
      case _ : UserInterruptException => System.exit(0); return -1;
    }
  }


}