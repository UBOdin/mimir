package mimir.util

import java.io.Reader
import org.jline.terminal.{Terminal,TerminalBuilder}
import org.jline.reader.{LineReader,LineReaderBuilder,EndOfFileException,UserInterruptException}
import com.typesafe.scalalogging.slf4j.LazyLogging

class LineReaderInputSource(
  val input: LineReader = LineReaderBuilder.builder().terminal(TerminalBuilder.terminal()).build()
)
  extends Reader
  with LazyLogging
{
  var pos: Int = -1;
  var curr: String = null;

  def close() = input.getTerminal.close
  def read(cbuf: Array[Char], offset: Int, len: Int): Int =
  {
    try { 
      var i:Int = 0;
      logger.debug(s"being asked for $len characters")
      while(i < len){
        while(curr == null || pos >= curr.length){
          if(i > 0){ logger.debug(s"returning $i characters"); return i; }
          curr = input.readLine("mimir> ")
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