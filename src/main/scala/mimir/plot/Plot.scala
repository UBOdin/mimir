package mimir.plot;

import java.io.File
import scala.io._

import scala.sys.process._
import scala.language.postfixOps
import scala.collection.JavaConverters._
import com.typesafe.scalalogging.slf4j.LazyLogging

import org.sameersingh.scalaplot._
import org.sameersingh.scalaplot.Style._
import org.sameersingh.scalaplot.Implicits._

import mimir._
import mimir.algebra._
import mimir.exec._
import mimir.exec.result._
import mimir.util._

/**
 * Plotting using Sameer Sing's ScalaPlot/Gnuplot
 * 
 * Includes Gnuplot configuration material inspired by 
 * Brighten Godfrey's blog post:
 * http://youinfinitesnake.blogspot.com/2011/02/attractive-scientific-plots-with.html
 */
object Plot
  extends LazyLogging
{

  val defaultFormats = Seq(
    PlotConfig( Color.Red,       PointType(1) ),
    PlotConfig( Color.DarkGreen, PointType(6) ),
    PlotConfig( Color.SteelBlue, PointType(2) ),
    PlotConfig( Color.Purple,    PointType(9) )
  )



  def value: (PrimitiveValue => Object) = {
    case NullPrimitive() => 0:java.lang.Long
    case IntPrimitive(l) => l:java.lang.Long
    case FloatPrimitive(f) => f:java.lang.Double
    case StringPrimitive(s) => s
    case d:DatePrimitive => d.asDateTime
    case t:TimestampPrimitive => t.asDateTime
  }

  def plot(input: ResultIterator, table: String, x: String, ys: Seq[String], console: OutputFormat)
  {
    val raw = input.toSeq
    val extracted: Seq[XYSeries] = 
      ys.zip(defaultFormats).map { case (y, format) => 
        val data = raw.flatMap { row => 
            (row(x), row(y)) match {
              case (_:NullPrimitive, _) => None
              case (_, _:NullPrimitive) => None
              case (xval:NumericPrimitive, yval:NumericPrimitive) => Some( (xval.asDouble, yval.asDouble) )
              case (xval, yval) => throw new RAException(s"Invalid Data Point: < $x: $xval, $y: $yval >")
            }
          }.toIndexedSeq
        logger.debug(s"For < $x, $y >: ${data.size} records with $format")
        XY(
          data,
          color = format.color,
          lw = 2,
          pt = format.pointType,
          label = y
        )
      }

    val data: XYData = extracted
    val chart: XYChart = data

    chart.showLegend = ys.size > 1
    chart.x.label = x
    chart.y.label = ys.mkString(", ")

  }

  def generate(chart: XYChart, name: String, console: OutputFormat)
  {
    System.getenv().get("TERM_PROGRAM") match 
    {
      case "iTerm.app" => {
        output(PNG("./", name), chart)
        inline(new File(name+".png"), console)
      }
      case _ => {
        output(PDF("./", name), chart)
        open(new File(name+".pdf"))
      }
    }
  }

  def open(f: File)
  {
    try {
      Process(s"open ${f}")!!
    } catch {
      case e:Exception => logger.debug(s"Can't open: ${e.getMessage}")
    }
  }

  def inline(f: File, console: OutputFormat)
  {
    console.printRaw(
      Array[Byte](
        '\u001b',
        ']',
        '1', '3', '3', '7', ';',
        'F', 'i', 'l', 'e', '='
      ) ++ f.getName.getBytes ++
      Array[Byte](
        ';','i','n','l','i','n','e','=','1',';',':'
      ) ++ mimir.util.SerializationUtils.b64encode(f).getBytes ++
      Array[Byte](
        '\n','\u0007'
      )
    )
    console.print("\n")
  }

  def getXY(input: Seq[Row], x: String, y: String): Seq[(Double,Double)] =
    input.flatMap { row => 
      (row(x), row(y)) match {
          case (_:NullPrimitive, _) => None
          case (_, _:NullPrimitive) => None
          case (xval:NumericPrimitive, yval:NumericPrimitive) => Some( (xval.asDouble, yval.asDouble) )
          case (xval, yval) => throw new RAException(s"Invalid Data Point: < $x: $xval, $y: $yval >")
        }
      }

  def plot(spec: mimir.sql.DrawPlot, db: Database, console: OutputFormat)
  {
    val convertConfig = (in:java.util.Map[String,net.sf.jsqlparser.expression.PrimitiveValue]) => {
      in.asScala.mapValues { db.sql.convert(_) }.toMap
    }

    var dataQuery = spec.getSource match {
      case q:net.sf.jsqlparser.statement.select.SubSelect => 
        db.sql.convert(q.getSelectBody())
      case q:net.sf.jsqlparser.schema.Table => 
        db.getTableOperator(SqlUtils.canonicalizeIdentifier(q.getName()))
    }
    val globalSettings = convertConfig(spec.getConfig())
    val lines: Seq[(String, String, Map[String, PrimitiveValue])] = 
      if(spec.getLines.isEmpty){
        val columns = db.bestGuessSchema(dataQuery).
          filter { case (_, TFloat()) | (_, TInt()) => true; case _ => false }.
          map(_._1)
        if(columns.isEmpty){
          throw new RAException(s"No valid columns for plotting: ${db.bestGuessSchema(dataQuery).map { x => x._1+":"+x._2 }.mkString(",")}")
        }
        val x = columns.head
        logger.info(s"No explicit columns given, implicitly using X = $x, Y = [${columns.tail.mkString(", ")}]")
        columns.tail.map { y => 
          (x, y, Map("TITLE" -> StringPrimitive(y)))
        }
      } else {
        val sch = dataQuery.schema
        var extraColumnCounter = 0;

        val convertExpression = (raw: net.sf.jsqlparser.expression.Expression) => {
          db.sql.convert(raw, { x => x }) match {
            case Var(vn) => vn
            case expr => {
              extraColumnCounter += 1
              val column = s"PLOT_EXPRESSION_$extraColumnCounter"
              dataQuery = OperatorUtils.projectInColumn(column, expr, dataQuery)
              column
            }
          }
        }

        spec.getLines.asScala.map { line =>
          val x = convertExpression(line.getX())
          val y = convertExpression(line.getY())
          val args = convertConfig(line.getConfig())
          (x, y, args)
        }
      }

    logger.info("QUERY: $dataQuery")

    db.query(dataQuery) { resultsRaw =>
      // We'll need multiple scans, so sequencify the results
      val results = resultsRaw.toSeq

      val data: XYData = 
        lines.zip(defaultFormats).map { case ((x, y, config), default) =>
          XY(
            getXY(results, x, y),
            color = default.color,
            lw = 2,
            pt = default.pointType,
            label = config.getOrElse("TITLE", StringPrimitive(y)).asString
          )
        }

      val chart: XYChart = data

      chart.showLegend = lines.size > 1
      globalSettings.get("XLABEL").foreach { arg => chart.x.label = arg.asString }
      globalSettings.get("YLABEL").foreach { arg => chart.y.label = arg.asString }

      val title = globalSettings.getOrElse("TITLE", StringPrimitive(QueryNamer(dataQuery))).asString

      generate(chart, title, console)
    }

  }
}

case class PlotConfig(color: Color.Value, pointType: PointType.Value)
{
  override def toString: String = s" { color : $color, pt : $pointType }"
}