package mimir.plot

import mimir.Database
import mimir.algebra._
import com.typesafe.scalalogging.slf4j.LazyLogging

object Heuristics 
  extends LazyLogging
{
  
  def pickDefaultLines(
    dataQuery: Operator,
    globalSettings: Plot.Config,
    db: Database
  ): (Operator, Seq[Plot.Line], Plot.Config) = 
  {
    //if no lines are specified, try to find the best ones
    val columns = db.bestGuessSchema(dataQuery)
    val columnMap = columns.toMap
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
      (
        Sort(Seq(SortColumn(Var(x), true)), dataQuery),
        Seq( (x, "MIMIR_PLOT_CDF", Map("TITLE" -> StringPrimitive(x))) ),
        Map(
          "XLABEL" -> StringPrimitive(x),
          "YLABEL" -> StringPrimitive("CDF")
        ) ++ globalSettings
      )
    } else {
      // TODO: Plug DetectSeries in here.
      logger.info(s"No explicit columns given, implicitly using X = $x, Y = [${numericColumns.tail.mkString(", ")}]")
      val commonType = 
        Typechecker.leastUpperBound(numericColumns.tail.map { y => columnMap(y) })
      (
        dataQuery,
        numericColumns.tail.map { y =>
          (x, y, Map("TITLE" -> StringPrimitive(y)))
        },
        Map(
          "XLABEL" -> StringPrimitive(x)
        ) ++ (commonType match { 
          case Some(TUser(utype)) => Map("YLABEL" -> StringPrimitive(utype))
          case Some(TDate()     ) => Map("YLABEL" -> StringPrimitive("Date"))
          case Some(TTimestamp()) => Map("YLABEL" -> StringPrimitive("Time"))
          case _                  => Map()
        }) ++ globalSettings
      )
    }
  }

  def applyLineDefaults(
    lines: Seq[Plot.Line], 
    globalSettings: Plot.Config,
    db: Database
  ): Seq[Plot.Line] = 
  {
    val linesHaveThanOneX = lines.map(_._1).toSet.size > 1

    lines.map { case (x, y, lineConfig) =>
      (x, y, 
        Map(
          "TITLE" -> StringPrimitive( 
            if(linesHaveThanOneX) { s"($x, $y)" }
            else { y }
          )
        ) ++ lineConfig
      )
    }
  }

}