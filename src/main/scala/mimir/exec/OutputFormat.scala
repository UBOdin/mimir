package mimir.exec

import org.jline.terminal.Terminal
import org.jline.utils.{AttributedStyle,AttributedStringBuilder,AttributedString}

import mimir.util.ExperimentalOptions

trait OutputFormat
{
  def print(msg: String): Unit
  def print(results: ResultIterator): Unit
}

object DefaultOutputFormat
  extends OutputFormat
{
  def print(msg: String) 
  {
    println(msg);
  }
  def print(result: ResultIterator)
  {
    ExperimentalOptions.ifEnabled("SILENT-TEST", () => {
      var x = 0
      while(result.getNext()){ x += 1; if(x % 10000 == 0) {println(s"$x rows")} }
      val missingRows = result.missingRows()
      println(s"Total $x rows; Missing: $missingRows")
    }, () => {
      println(result.schema.map( _._1 ).mkString(","))
      println("------")
      result.foreachRow { row => println(row.rowString) }
      if(result.missingRows()){
        println("( There may be missing result rows )")
      }
    })
  }
}

class PrettyOutputFormat(terminal: Terminal)
  extends OutputFormat
{
  def print(msg: String)
  {
    terminal.writer.write(msg)
    if(msg.length <= 0  || msg.charAt(msg.length - 1) != '\n'){ terminal.writer.write('\n') }
  }
  def print(result: ResultIterator)
  {
    terminal.writer.write(result.schema.map( _._1 ).mkString(",")+"\n")
    terminal.writer.write("------\n")
    result.foreachRow { row => 
      val currLine = new AttributedStringBuilder(200)
      val lineStyle = 
        if(row.deterministicRow()){ AttributedStyle.DEFAULT }
        else { AttributedStyle.DEFAULT.faint().underline() }

      var sep = ""

      for(i <- (0 until row.numCols)){
        currLine.append(sep, lineStyle)
        currLine.append(
          row(i).toString,
          if(row.deterministicCol(i)){ lineStyle } 
            else { lineStyle.foreground(AttributedStyle.RED) }
        )
        sep = ","
      }
      currLine.append("\n")

      val lineData = 
      terminal.writer.write(
        currLine.toAttributedString.toAnsi(terminal)
      )
    }
  }

}