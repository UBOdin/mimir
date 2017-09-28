package mimir.exec

import scala.collection.mutable.Buffer
import org.jline.terminal.Terminal
import org.jline.utils.{AttributedStyle,AttributedStringBuilder,AttributedString}

import mimir.util.ExperimentalOptions
import mimir.exec.result.{ResultIterator,Row}

trait OutputFormat
{
  def print(msg: String): Unit
  def print(results: ResultIterator): Unit
  def printRaw(msg: Array[Byte]): Unit
}

object DefaultOutputFormat
  extends OutputFormat
{
  def print(msg: String) 
  {
    println(msg);
  }
  def print(row: Row)
  {
    println(
      row.tuple.zipWithIndex.map { case (field, idx) => 
        field.toString + (
          if(row.isColDeterministic(idx)){ "" } else { "*" }
        )
      }.mkString(",") + (
        if(row.isDeterministic){ "" } else { " (This row may be invalid)" }
      )
    )
  }
  def print(output: ResultIterator)
  {
    ExperimentalOptions.ifEnabled("SILENT-TEST", () => {
      var x = 0
      output.foreach { row => x += 1; if(x % 10000 == 0) {println(s"$x rows")} }
      println(s"Total $x rows")
    }, () => {
      println(output.schema.map( _._1 ).mkString(","))
      println("------")
      output.foreach { row => print(row) }
    })
  }

  def printRaw(msg: Array[Byte])
  {
    println(msg.mkString)
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

  def print(output: ResultIterator)
  {
    val header:Seq[String] = output.schema.map( _._1 ).map { x => if (x == null){ "??" } else { x }}
    var spacing = scala.collection.mutable.Seq[Int]()++header.map(_.length)
    var results:Seq[Row] = output.toSeq
    var haveUncertainLine = false;
    var haveUncertainValue = false;

    for( row <- results ) {
      for(i <- 0 until spacing.size){
        val fieldString = row(i).toString
        if(fieldString.length > spacing(i)){
          spacing(i) = fieldString.length
        }
      }
    }

    terminal.writer.write(" "+
      header.zip(spacing).map { case (title, width) =>
        title.padTo(width, ' ')
      }.mkString(" | ")+" \n"
    );
    terminal.writer.write(
      spacing.map { width => "".padTo(width+2, '-') }.mkString("+")+"\n"
    )

    for( row <- results ) {
      var sep = " "
      val lineStyle = 
        if(row.isDeterministic){ AttributedStyle.DEFAULT }
        else { 
          haveUncertainLine = true; 
          AttributedStyle.DEFAULT.faint().underline() 
        }
      val line = new AttributedStringBuilder(200)
      for( i <- 0 until spacing.size ){
        line.append(sep, lineStyle)
        line.append(
          row(i).toString.padTo(spacing(i), ' '),
          if(row.isColDeterministic(i)){ lineStyle } 
          else { 
            haveUncertainValue = true;
            lineStyle.foreground(AttributedStyle.RED) 
          }
        )
        sep = " | "
      }
      line.append("\n")
      terminal.writer.write(
        line.toAttributedString.toAnsi(terminal)
      )
    }

    if(haveUncertainValue || haveUncertainLine){
      val line = new AttributedStringBuilder(200)
      line.append("I had to make guesses about some ")
      if(haveUncertainValue){
        line.append("values", AttributedStyle.DEFAULT.foreground(AttributedStyle.RED))
      }
      if(haveUncertainValue && haveUncertainLine){
        line.append(" and ")
      }
      if(haveUncertainLine){
        line.append("rows of data", AttributedStyle.DEFAULT.faint().underline())
      }
      line.append(" that I just showed you.  You can use `ANALYZE` to see them.\n");
      terminal.writer.write(
        line.toAttributedString.toAnsi(terminal)
      )
    }
  }

  def printRaw(msg: Array[Byte])
  {
    terminal.output.write(msg)
  }
}