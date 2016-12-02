package mimir.util

import com.typesafe.scalalogging.slf4j.LazyLogging

import java.io.{File, FileReader, BufferedReader}
import java.sql.SQLException

import mimir.Database
import mimir.algebra.Type

import scala.collection.mutable.ListBuffer

object LoadCSV extends LazyLogging {

  def handleLoadTable(db: Database, targetTable: String, sourceFile: File): Unit =
    handleLoadTable(db, targetTable, sourceFile, x => true)

  def handleLoadTable(db: Database, targetTable: String, sourceFile: File, detectHeader: String => Boolean){
    val input = new BufferedReader(new FileReader(sourceFile))
    val firstLine = input.readLine()

    db.getTableSchema(targetTable) match {

      case Some(sch) =>
        if(detectHeader(firstLine)) {
          populateTable(db, input, targetTable, sch) // Ignore header since table already exists
        } else {
          populateTable(
            db,
            new BufferedReader(new FileReader(sourceFile)), // Reset to top
            targetTable,
            sch
          )
        }

      case None =>
        if(detectHeader(firstLine)) {
          val columnNames = 
            firstLine.
              split(",").
              map( sanitizeColumnName _ )

          val dupColumns = 
            columnNames.
              groupBy( x => x ).
              filter( _._2.length > 1 ).
              map(_._1).
              toSet

          val uniqueColumnNames =
            if(dupColumns.isEmpty){ columnNames }
            else {
              var idx = scala.collection.mutable.Map[String,Int]()
              idx ++= dupColumns.map( (_, 0) ).toMap

              columnNames.map( x =>
                if(idx contains x){ idx(x) += 1; x+"_"+idx(x) }
                else { x }
              )
            }

          db.backend.update("CREATE TABLE "+targetTable+"("+
              uniqueColumnNames.map(_+" varchar").
              mkString(", ") +
            ")")

          handleLoadTable(db, targetTable, sourceFile)
        }
        else {
          throw new SQLException("No header supplied for creating new table")
        }
    }
  }

  def sanitizeColumnName(name: String): String =
  {
    name.
      replace("^([0-9])","X\1").        // Prefix leading digits with an 'X'
      replaceAll("[^a-zA-Z0-9]+", "_"). // Replace sequences of non-alphanumeric characters with underscores
      replaceAll("_+$", "").            // Strip trailing underscores
      toUpperCase                       // Capitalize
  }

  private def populateTable(db: Database,
                            src: BufferedReader,
                            targetTable: String,
                            sch: List[(String, Type.T)]): Unit = {
    val keys = 
      sch.
        map(_._1).
        // map((x) => "\'"+x+"\'").
        mkString(", ")

    val statements = new ListBuffer[String]()

    val doInsert = () => {
      if(statements.nonEmpty) {
        val cmd = 
          s"INSERT INTO $targetTable($keys) VALUES "+statements.mkString(",")
        try {
          logger.trace(s"INSERT: $cmd")
          db.backend.update(cmd)
        } catch {
          case (e: SQLException) => 
            throw new SQLException("Bulk Load Error: "+cmd, e)
        }
      }
      statements.clear()
    }   

    var line = src.readLine()

    while(line != null){
      if(statements.size >= 1){ doInsert() }

      val dataLine = line.trim.split(",").padTo(sch.size, "")
      val data = dataLine.zip(sch).map({ 
          case ("", _) => "null"
          case (x, (_, (Type.TDate | Type.TString))) => "\'"+x+"\'"
          case (x, (_, Type.TInt))   if x.matches("^[+-]?[0-9]+$") => x
          case (x, (_, Type.TFloat)) if x.matches("^[+-]?[0-9]+([.][0-9]+)?(e[+-]?[0-9]+)?$") => x
          case (x, (c,t)) => logger.warn(s"Don't know how to deal with $c ($t): $x, using null instead"); null
      }).mkString(", ")

      statements.append(s"($data)")
      logger.debug(s"INSERT: $data")
      line = src.readLine()
    }
    doInsert()
  }
}
