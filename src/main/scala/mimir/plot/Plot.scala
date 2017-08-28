package mimir.plot;

import java.io.File
import scala.io._
import play.api.libs.json._

import scala.sys.process._
import scala.language.postfixOps
import scala.collection.JavaConverters._
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir._
import mimir.algebra._
import mimir.exec._
import mimir.exec.result._
import mimir.util._
import mimir.util.JsonUtils.primitiveValueWrites

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

  def value: (PrimitiveValue => Object) = {
    case NullPrimitive() => 0:java.lang.Long
    case IntPrimitive(l) => l:java.lang.Long
    case FloatPrimitive(f) => f:java.lang.Double
    case StringPrimitive(s) => s
    case d:DatePrimitive => d.asDateTime
    case t:TimestampPrimitive => t.asDateTime
    case i:IntervalPrimitive => i.asInterval
    case BoolPrimitive(true) => 1:java.lang.Long
    case BoolPrimitive(false) => 0:java.lang.Long
    case RowIdPrimitive(s) => s
    case TypePrimitive(t) => t
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
        db.table(SqlUtils.canonicalizeIdentifier(q.getName()))
    }
    val globalSettings = convertConfig(spec.getConfig())

    val lines: Seq[(String, String, Map[String, PrimitiveValue])] =
      if(spec.getLines.isEmpty){
          //if no lines are specified, try to find the best ones
        val columns = db.bestGuessSchema(dataQuery).
          filter { case (_, TFloat()) | (_, TInt()) => true; case _ => false }.
          map(_._1)
          //if that comes up with nothing either, then throw an exception
        if(columns.isEmpty){
          throw new RAException(s"No valid columns for plotting: ${db.bestGuessSchema(dataQuery).map { x => x._1+":"+x._2 }.mkString(",")}")
        }
        val x = columns.head
        if(columns.tail.isEmpty){
          dataQuery = Sort(Seq(SortColumn(Var(x), true)), dataQuery)
          Seq( ("MIMIR_PLOT_CDF", x, Map("TITLE" -> StringPrimitive(x))) )
        } else {
          logger.info(s"No explicit columns given, implicitly using X = $x, Y = [${columns.tail.mkString(", ")}]")
          columns.tail.map { y =>
            (x, y, Map("TITLE" -> StringPrimitive(y)))
          }
        }
      } else {
        val sch = db.typechecker.schemaOf(dataQuery)
        var extraColumnCounter = 0;

        val convertExpression = (raw: net.sf.jsqlparser.expression.Expression) => {
          db.sql.convert(raw, { x => x }) match {
            case Var(vn) => vn
            case expr => {
              extraColumnCounter += 1
              val column = s"PLOT_EXPRESSION_$extraColumnCounter"
              dataQuery = dataQuery.addColumn( column -> expr )
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

    logger.trace(s"QUERY: $dataQuery")

    db.query(dataQuery) { resultsRaw =>
      val results = resultsRaw.toIndexedSeq

      val globalSettingsJson = Json.toJson(
        globalSettings ++ 
        Map("DEFAULTSAVENAME" -> StringPrimitive(QueryNamer(dataQuery)))
      )

      val linesJson = Json.toJson(
        lines.map { case (x, y, config) => Json.arr(Json.toJson(x), Json.toJson(y), Json.toJson(config)) }
      )

      //get the defaultSaveName to pass to the python code for any naming defaults
      var defaultName= StringPrimitive(QueryNamer(dataQuery))
      var nameToWrite="(DEFAULTSAVENAME,"+defaultName+")\n";
      //define the processIO to feed data to the process
      var io=new ProcessIO(
         in=>{
            val data = Json.obj(
              "GLOBAL" -> globalSettingsJson,
              "LINES" -> linesJson,
              "RESULTS" -> Json.toJson(
                dataQuery.columnNames.map { col => col -> results.map { _(col) } }.toMap
              )
            )
            in.write(data.toString.getBytes)
            in.close();
         },
         out=>{ for(l <- Source.fromInputStream(out).getLines()){ console.printRaw(l.getBytes);console.print("\n"); }; out.close() },
         err=>{ for(l <- Source.fromInputStream(err).getLines()){ logger.debug(l) }; err.close() }
        )

        //run the python process using the ProcessIO
        val process = PythonProcess("plot", io)
        //wait for the process to finish....
        val exit=process.exitValue();
        if(exit != 0){ logger.error("Plot was unsuccessful.") }
        //fin
    }
  }
}
