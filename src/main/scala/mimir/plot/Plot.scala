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
      in.asScala.map { field => (field._1.toUpperCase -> db.sql.convert(field._2)) }.toMap
    }

    var dataQuery = spec.getSource match {
      case q:net.sf.jsqlparser.statement.select.SubSelect =>
        db.sql.convert(q.getSelectBody())
      case q:net.sf.jsqlparser.schema.Table => 
        db.table(SqlUtils.canonicalizeIdentifier(q.getName()))
    }
    var globalSettings = convertConfig(spec.getConfig())

    val lines: Seq[(String, String, Map[String, PrimitiveValue])] =
      if(spec.getLines.isEmpty){
          //if no lines are specified, try to find the best ones
        val columns = db.bestGuessSchema(dataQuery).toMap
        val numericColumns =
          columns.toSeq
            .filter { t => Type.isNumeric(t._2) }
            .map { _._1 }
          //if that comes up with nothing either, then throw an exception
        if(numericColumns.isEmpty){
          throw new RAException(s"No valid columns for plotting: ${db.bestGuessSchema(dataQuery).map { x => x._1+":"+x._2 }.mkString(",")}")
        }
        val x = numericColumns.head
        if(numericColumns.tail.isEmpty){
          dataQuery = Sort(Seq(SortColumn(Var(x), true)), dataQuery)
          globalSettings = Map(
            "XLABEL" -> StringPrimitive(x),
            "YLABEL" -> StringPrimitive("CDF")
          ) ++ globalSettings
          Seq( (x, "MIMIR_PLOT_CDF", Map("TITLE" -> StringPrimitive(x))) )
        } else {
          // TODO: Plug DetectSeries in here.
          logger.info(s"No explicit columns given, implicitly using X = $x, Y = [${numericColumns.tail.mkString(", ")}]")
          val commonType = 
            Typechecker.leastUpperBound(numericColumns.tail.map { y => columns(y) })
          globalSettings = Map(
            "XLABEL" -> StringPrimitive(x)
          ) ++ (commonType match { 
            case Some(TUser(utype)) => Map("YLABEL" -> StringPrimitive(utype))
            case Some(TDate()     ) => Map("YLABEL" -> StringPrimitive("Date"))
            case Some(TTimestamp()) => Map("YLABEL" -> StringPrimitive("Time"))
            case _                  => Map()
          }) ++ globalSettings
          numericColumns.tail.map { y =>
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

      val simplifiedQuery = db.compiler.optimize(dataQuery)
      logger.trace(s"QUERY: $simplifiedQuery")
      val globalSettingsJson = Json.toJson(
        globalSettings ++ 
        Map("DEFAULTSAVENAME" -> StringPrimitive(QueryNamer(simplifiedQuery)))
      )

      val linesJson = Json.toJson(
        lines.map { case (x, y, config) => Json.arr(Json.toJson(x), Json.toJson(y), Json.toJson(config)) }
      )

      val warningLines: JsValue = 
        globalSettings.getOrElse("WARNINGS", StringPrimitive("POINTS")).asString.toUpperCase match { 
          case "OFF" => JsNull
          case _ => {
            Json.toJson(
              lines.flatMap { case (x, y, _) =>
                results
                  .filter { row => 
                    (    (!row.isDeterministic()) 
                      || (!row.isColDeterministic(x))
                      || (!row.isColDeterministic(y))
                    )
                  }
                  .map { row => Json.arr(row(x), row(y)) }
              }
              .toSet
            )
          }
        }

      //define the processIO to feed data to the process
      var io=new ProcessIO(
         in=>{
            val data = Json.obj(
              "GLOBAL" -> globalSettingsJson,
              "LINES" -> linesJson,
              "RESULTS" -> Json.toJson(
                dataQuery.columnNames.map { col => col -> results.map { _(col) } }.toMap
              ),
              "WARNINGS" -> Json.toJson(
                warningLines
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
