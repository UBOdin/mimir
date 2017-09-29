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

  type Config = Map[String, PrimitiveValue]
  type Line = (String, String, Config)

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

    val lines: Seq[(String, String, Config)] =
      Heuristics.applyLineDefaults(
        if(spec.getLines.isEmpty){
          val (newDataQuery, chosenLines, newGlobalSettings) = 
            Heuristics.pickDefaultLines(dataQuery, globalSettings, db)

          dataQuery = newDataQuery
          globalSettings = newGlobalSettings

          // return the actual lines generated
          chosenLines
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
        },
        globalSettings,
        db
      )

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
              lines.flatMap { case (x, y, config) =>
                results
                  .filter { row => 
                    (    (!row.isDeterministic()) 
                      || (!row.isColDeterministic(x))
                      || (!row.isColDeterministic(y))
                    )
                  }
                  .map { row => 
                    logger.trace(s"${config("TITLE")} @ ${row(x)} - ${row(y)} -> row: ${row.isDeterministic()}, $x: ${row.isColDeterministic(x)} $y: ${row.isColDeterministic(y)}")
                    Json.arr(row(x), row(y)) 
                  }
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
