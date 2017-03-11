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
      LoadCSV.handleLoadTable(db, "RATINGS1", new File("test/data/ratings1.csv"))
      queryOneColumn("SELECT PID FROM RATINGS1") must contain(eachOf(
        str("P123"), str("P2345"), str("P124"), str("P325")
      ))
    }

    "Load CSV files without headers" >> {
      LoadCSV.handleLoadTable(db, "U", new File("test/r_test/u.csv"), false)
      val col1: String = db.getTableSchema("U").get.head._1
      queryOneColumn(s"SELECT $col1 FROM U") must contain(eachOf(
        str("1"), str("2"), str("3"), str("4"), str("5")
      ))
    }

    "Load CSV files with headers given an existing schema" >> {
      update("""
        CREATE TABLE RATINGS2(
          PID string,
          EVALUATION float,
          NUM_RATINGS float
        );
      """)
      LoadCSV.handleLoadTable(db, "RATINGS2", new File("test/data/ratings2.csv"))
      queryOneColumn(s"SELECT PID FROM RATINGS2") must contain(eachOf(
        str("P125"), str("P34234"), str("P34235")
      ))
      queryOneColumn(s"SELECT EVALUATION FROM RATINGS2") must contain(eachOf(
        f(3.0), f(5.0), f(4.5)
      ))
    }

    "Load CSV files without headers given an existing schema" >> {
      update("""
        CREATE TABLE U2(
          A int,
          B int,
          C int
        );
      """)
      LoadCSV.handleLoadTable(db, "U2", new File("test/r_test/u.csv"), false)
      val col1: String = db.getTableSchema("U2").get.head._1
      queryOneColumn(s"SELECT $col1 FROM U2") must contain(eachOf(
        i(1), i(2), i(3), i(4), i(5)
      ))
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
      LoggerUtils.enhance("mimir.util.LoadCSV$", LoggerUtils.ERROR, () => {
        LoadCSV.handleLoadTable(db, "RATINGS1WITHTYPES", new File("test/data/ratings1.csv"))
      })
      queryOneColumn("SELECT PID FROM RATINGS1") must contain(eachOf(
        str("P123"), str("P2345"), str("P124"), str("P325")
      ))
      queryOneColumn("SELECT RATING FROM RATINGS1WITHTYPES") must contain(
        NullPrimitive()
      )
      queryOneColumn("SELECT REVIEW_CT FROM RATINGS1WITHTYPES") must contain(eachOf(
        f(50.0), f(245.0), f(100.0), f(30.0)
      ))
    }

    "Load CSV files with missing values" >> {
      LoadCSV.handleLoadTable(db, "R", new File("test/r_test/r.csv"), false)
      val colNames: Seq[String] = db.getTableSchema("R").get.map(_._1)
      val b = colNames(1)
      val c = colNames(2)
      queryOneColumn(s"SELECT $b FROM R") must contain(NullPrimitive())
      queryOneColumn(s"SELECT $c FROM R") must contain(NullPrimitive())
    }

    "Load CSV files with garbled data" >> {
      LoggerUtils.enhance("mimir.util.NonStrictCSVParser", LoggerUtils.ERROR, () => {
        LoadCSV.handleLoadTable(db, "GARBLED", new File("test/data/garbledRatings.csv"))
      })
      queryOneColumn("SELECT PID FROM GARBLED") must contain(
        eachOf(str("P123"), str("P124"))
      )
      queryOneColumn("SELECT PID FROM GARBLED") must have size(4)
    }

  }

}