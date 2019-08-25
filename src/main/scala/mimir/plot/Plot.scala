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
import mimir.serialization.AlgebraJson._
import mimir.parser.DrawPlot
import mimir.sql.SqlToRA

object Plot
  extends LazyLogging
{

  type Config = Map[String, PrimitiveValue]
  type Line = (ID, ID, Config)

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

  def plot(spec: DrawPlot, db: Database, console: OutputFormat)
  {
    val convertConfig = (in: Seq[(String,sparsity.expression.PrimitiveValue)]) => {
      in.map { case (arg, value) => (arg.toUpperCase -> db.sqlToRA(value)) }.toMap
    }

    var dataQuery = spec.body match {
      case q:sparsity.select.FromSelect =>
        db.sqlToRA(q.body)
      case sparsity.select.FromTable(schema, table, _) => 
        db.catalog.tableOperator(table)
      case q:sparsity.select.FromJoin => ???
    }
    var globalSettings = convertConfig(spec.args)

    val lines: Seq[(ID, ID, Config)] =
      Heuristics.applyLineDefaults(
        if(spec.lines.isEmpty){
          val (newDataQuery, chosenLines, newGlobalSettings) = 
            Heuristics.pickDefaultLines(dataQuery, globalSettings, db)

          dataQuery = newDataQuery
          globalSettings = newGlobalSettings

          // return the actual lines generated
          chosenLines
        } else {
          val sch = db.typechecker.schemaOf(dataQuery)
          var extraColumnCounter = 0;

          val convertExpression:(sparsity.expression.Expression => ID) = 
            (raw: sparsity.expression.Expression) => {
            db.sqlToRA(raw, SqlToRA.literalBindings) match {
              case Var(vn) => vn
              case expr => {
                extraColumnCounter += 1
                val column = ID(s"PLOT_EXPRESSION_$extraColumnCounter")
                dataQuery = dataQuery.addColumnsByID( column -> expr )
                column
              }
            }
          }

          spec.lines.map { line =>
            val x = convertExpression(line.x)
            val y = convertExpression(line.x)
            val args = convertConfig(line.args)
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
        Map("DEFAULTSAVENAME" -> StringPrimitive(QueryNamer(simplifiedQuery).id))
      )

      val linesJson = Json.toJson(
        lines.map { case (x, y, config) => Json.arr(Json.toJson(x.id), Json.toJson(y.id), Json.toJson(config)) }
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
                dataQuery.columnNames.map { col => col.id -> results.map { _(col) } }.toMap
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
