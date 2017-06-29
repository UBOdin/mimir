package mimir.util

import java.io.{Reader,File}
import org.jline.terminal.{Terminal,TerminalBuilder}
import org.jline.reader.{LineReader,LineReaderBuilder,EndOfFileException,UserInterruptException}
import com.typesafe.scalalogging.slf4j.LazyLogging

class LineReaderInputSource(terminal: Terminal)
  extends Reader
  with LazyLogging
{
  private val historyFile = System.getProperty("user.home") + File.separator + ".mimir_history"
  val input: LineReader = 
    LineReaderBuilder.
      builder().
      terminal(terminal).
      variable(LineReader.HISTORY_FILE, historyFile).
      build()
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