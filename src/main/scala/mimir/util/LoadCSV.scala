package mimir.util

import java.io.{File, FileReader, BufferedReader}
import java.sql.SQLException

import mimir.Database
import mimir.algebra.Type

import scala.collection.mutable.ListBuffer

object LoadCSV {

  def handleLoadTable(db: Database, targetTable: String, sourceFile: File){
    val input = new BufferedReader(new FileReader(sourceFile))
    val firstLine = input.readLine()

    db.getTableSchema(targetTable) match {

      case Some(sch) =>
        if(headerDetected(firstLine)) {
          populateTable(db, input, targetTable, sch) // Ignore header since table already exists
        }
        else {
          populateTable(
            db,
            new BufferedReader(new FileReader(sourceFile)), // Reset to top
            targetTable,
            sch
          )
        }

      case None =>
        if(headerDetected(firstLine)) {
          db.backend.update("CREATE TABLE "+targetTable+"("+
            firstLine.split(",").map((x) => "\'"+x.trim.replace(" ", "")+"\'" ).mkString(" varchar, ")+
            " varchar)")

          handleLoadTable(db, targetTable, sourceFile)
        }
        else {
          throw new SQLException("No header supplied for creating new table")
        }
    }
  }

  /**
   * A placeholder method for an unimplemented feature
   *
   * During CSV load, a file may have a header, or not
   */
  private def headerDetected(line: String): Boolean = {
    if(line == null) return false

    // TODO Detection logic

    true // For now, assume every CSV file has a header
  }

  private def populateTable(db: Database,
                            src: BufferedReader,
                            targetTable: String,
                            sch: List[(String, Type.T)]): Unit = {
    val keys = sch.map(_._1).map((x) => "\'"+x+"\'").mkString(", ")
    val statements = new ListBuffer[String]()

    while(true){
      val line = src.readLine()
      if(line == null) { if(statements.nonEmpty) db.backend.update(statements.toList); return }

      val dataLine = line.trim.split(",").padTo(sch.size, "")
      val data = dataLine.indices.map( (i) =>
        dataLine(i) match {
          case "" => null
          case x => sch(i)._2 match {
            case Type.TDate | Type.TString => "\'"+x+"\'"
            case _ => x
          }
        }
      ).mkString(", ")

      statements.append("INSERT INTO "+targetTable+"("+keys+") VALUES ("+data+")")
    }
  }
}
