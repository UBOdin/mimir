package mimir.load

import java.io._
import org.specs2.mutable._
import org.specs2.matcher.FileMatchers
import mimir.test._
import mimir.algebra._
import mimir.util._

object LoadCSVSpec extends SQLTestSpecification("LoadCSV")
{
  "LoadCSV" should {

    "Load CSV files with headers" >> {
      LoadCSV(db, "RATINGS1", new File("test/data/ratings1.csv"))
      queryOneColumn("SELECT PID FROM RATINGS1"){
        _.toSeq must contain(
          str("P123"), str("P2345"), str("P124"), str("P325")
        )
      }
    }

    "Load CSV files without headers" >> {
      LoadCSV(db, "U", new File("test/r_test/u.csv"), Map("HEADER" -> "NO"))
      val col1: String = db.tableSchema("U").get.head._1
      queryOneColumn(s"SELECT $col1 FROM U"){
        _.toSeq must contain(
          str("1"), str("2"), str("3"), str("4"), str("5")
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
      LoadCSV(db, "RATINGS2", new File("test/data/ratings2.csv"))
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
      LoadCSV(db, "U2", new File("test/r_test/u.csv"), Map("HEADER" -> "NO"))
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
        LoadCSV(db, "RATINGS1WITHTYPES", new File("test/data/ratings1.csv"))
      }
      queryOneColumn("SELECT PID FROM RATINGS1"){ 
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
      LoadCSV(db, "R", new File("test/r_test/r.csv"), Map("HEADER" -> "NO"))
      val colNames: Seq[String] = db.tableSchema("R").get.map(_._1)
      val b = colNames(1)
      val c = colNames(2)
      queryOneColumn(s"SELECT $b FROM R"){ _.toSeq must contain(NullPrimitive()) }
      queryOneColumn(s"SELECT $c FROM R"){ _.toSeq must contain(NullPrimitive()) }
    }

    "Load CSV files with garbled data" >> {
      LoggerUtils.error("mimir.util.NonStrictCSVParser") {
        LoadCSV(db, "GARBLED", new File("test/data/garbledRatings.csv"))
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
        LoadCSV(db, "EMPLOYEE", new File("test/data/Employee.csv"))
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