package mimir.util

import java.io._
import org.specs2.mutable._
import org.specs2.matcher.FileMatchers
import mimir.test._
import mimir.algebra._

object LoadCSVSpec extends SQLTestSpecification("LoadCSV")
{
  "LoadCSV" should {

    "Load CSV files with headers" >> {
      db.loadTable(new File("test/data/ratings1.csv"))
      queryOneColumn("SELECT PID FROM RATINGS1"){
        _.toSeq must contain(
          str("P123"), str("P2345"), str("P124"), str("P325")
        )
      }
    }

    "Load CSV files without headers" >> {
      db.loadTable(new File("test/r_test/u.csv"))
      val col1: String = db.tableSchema("U").get.head._1
      queryOneColumn(s"SELECT $col1 FROM U"){
        _.toSeq must contain(
          i(1), i(2), i(3), i(4), i(5)
        )
      }
    }

    "Load CSV files with headers given an existing schema" >> {
      update("""
        CREATE TABLE RATINGS2(
          PID string,
          EVALUATION float,
          NUM_RATINGS float
        );
      """)
      db.loadTable(new File("test/data/ratings2.csv"), allowAppend = true)
      queryOneColumn(s"SELECT PID FROM RATINGS2"){ 
        _.toSeq must contain(
          str("P125"), str("P34234"), str("P34235")
        )
      }
      queryOneColumn(s"SELECT EVALUATION FROM RATINGS2"){ 
        _.toSeq must contain(
          f(3.0), f(5.0), f(4.5)
        )
      }
    }

    "Load CSV files without headers given an existing schema" >> {
      update("""
        CREATE TABLE U2(
          A int,
          B int,
          C int
        );
      """)
      db.loadTable(new File("test/r_test/u.csv"), targetTable = "U2", allowAppend = true)
      val col1: String = db.tableSchema("U2").get.head._1
      queryOneColumn(s"SELECT $col1 FROM U2"){
        _.toSeq must contain(
          i(1), i(2), i(3), i(4), i(5)
        )
      }
    }

    "Load CSV files that have type errors" >> {
      update("""
        CREATE TABLE RATINGS1WITHTYPES(
          PID string,
          RATING float,
          REVIEW_CT float
        );
      """)
      // Disable warnings for type errors
      LoggerUtils.error("mimir.util.LoadCSV$"){
        db.loadTable( new File("test/data/ratings1.csv"), "RATINGS1WITHTYPES", allowAppend = true )
      }
      queryOneColumn("SELECT PID FROM RATINGS1WITHTYPES"){ 
        _.toSeq must contain(
          str("P123"), str("P2345"), str("P124"), str("P325")
        )
      }
      queryOneColumn("SELECT RATING FROM RATINGS1WITHTYPES"){
        _.toSeq must contain(
          NullPrimitive()
        )
      }
      queryOneColumn("SELECT REVIEW_CT FROM RATINGS1WITHTYPES"){
        _.toSeq must contain(
          f(50.0), f(245.0), f(100.0), f(30.0)
        )
      }
    }

    "Load CSV files with missing values" >> {
      db.loadTable( new File("test/r_test/r.csv") )
      val colNames: Seq[String] = db.tableSchema("R").get.map(_._1)
      val b = colNames(1)
      val c = colNames(2)
      queryOneColumn(s"SELECT $b FROM R"){ _.toSeq must contain(NullPrimitive()) }
      queryOneColumn(s"SELECT $c FROM R"){ _.toSeq must contain(NullPrimitive()) }
    }

    "Load CSV files with garbled data" >> {
      LoggerUtils.error("mimir.util.NonStrictCSVParser") {
        db.loadTable( new File("test/data/garbledRatings.csv"), targetTable = "GARBLED" )
      }
      queryOneColumn("SELECT PID FROM GARBLED"){
        _.toSeq must contain(
          eachOf(str("P123"), str("P124"))
        )
      }
      queryOneColumn("SELECT PID FROM GARBLED"){
        _.toSeq must have size(4)
      }
    }

    "Load Dates Properly" >> {
      update("""
        CREATE TABLE EMPLOYEE(
          Name string, 
          Age int,
          JoinDate date,
          Salary float,
          Married bool
        )
      """)
      LoggerUtils.error("mimir.util.NonStrictCSVParser") {
        db.loadTable( 
          new File("test/data/Employee.csv"), 
          targetTable = "EMPLOYEE", 
          allowAppend = true
        )
      }
      db.backend.resultRows("""
        SELECT cast(JOINDATE as varchar) FROM EMPLOYEE
      """).map{ _(0).asString } must contain(
        "2011-08-01",
        "2014-06-19",
        "2007-11-11",
        "2005-01-11"
      )
    }

  }

}