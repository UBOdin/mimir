package mimir.exec

import scala.collection.mutable.Buffer
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

  type ResultRow = (Seq[(String,Boolean)], Boolean)

  def print(result: ResultIterator)
  {
    val header:Seq[String] = result.schema.map( _._1 )
    val spacing = scala.collection.mutable.Seq[Int]()++header.map(_.length)
    var results:Iterable[ResultRow] = 
      result.mapRows { row => 
        ((0 until row.numCols).map { i =>
          val cell = row(i).toString
          (cell, row.deterministicCol(i))
        }, row.deterministicRow())
      }
    for( (fields, _) <- results) {
      for(i <- 0 until spacing.size){
        val fieldString = fields(i)._1.toString
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

    for( (fields, rowDet) <- results ) {
      var sep = " "
      val lineStyle = 
        if(rowDet){ AttributedStyle.DEFAULT }
        else { AttributedStyle.DEFAULT.faint().underline() }
      val currLine = new AttributedStringBuilder(200)
      for( ((field, fieldDet), width) <- fields.zip(spacing) ){
        currLine.append(sep, lineStyle)
        currLine.append(
          field.toString.padTo(width, ' '),
          if(fieldDet){ lineStyle } 
            else { lineStyle.foreground(AttributedStyle.RED) }
        )
        sep = " | "
      }
      currLine.append("\n")
      terminal.writer.write(
        currLine.toAttributedString.toAnsi(terminal)
      )
    }
  }

}