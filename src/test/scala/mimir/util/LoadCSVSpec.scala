package mimir.util

import java.io._
import org.specs2.mutable._
import org.specs2.matcher.FileMatchers
import mimir.test._
import mimir.algebra._
import mimir.parser.MimirSQL

object LoadCSVSpec extends SQLTestSpecification("LoadCSV")
{
  
  "LoadCSV" should {
    "Load CSV files with headers" >> {
      loadCSV("RATINGS1", "test/data/ratings1.csv")
      queryOneColumn("SELECT PID FROM RATINGS1"){
        _.toSeq must contain(
          str("P123"), str("P2345"), str("P124"), str("P325")
        )
      }
    }

    "Load CSV files without headers" >> {
      loadCSV( "U", "test/r_test/u.csv" )
      val col1: String = db.tableSchema("U").get.head._1.id
      queryOneColumn(s"SELECT $col1 FROM U"){
        _.toSeq must contain(
          i(1), i(2), i(3), i(4), i(5)
        )
      }
    }

    "Load CSV files with headers given an existing schema" >> {
      loadCSV("RATINGS2", 
        Seq(
          ("PID","string"), 
          ("EVALUATION","float"), 
          ("NUM_RATINGS","float")
        ), 
        "test/data/ratings2.csv"
      )
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
      loadCSV( "U2", Seq(("A","int"), ("B","int"), ("C","int")), "test/r_test/u.csv")
      val col1: String = db.tableSchema("U2").get.head._1.id
      queryOneColumn(s"SELECT $col1 FROM U2"){
        _.toList must contain(
          i(1), i(2), i(3), i(4), i(5)
        )
      }
    }

    "Load CSV files that have type errors" >> {
      // Disable warnings for type errors
      LoggerUtils.error("mimir.util.LoadCSV$"){
        loadCSV( "RATINGS1WITHTYPES", Seq(("PID","string"),("RATING","float"),("REVIEW_CT","float")), "test/data/ratings1.csv")
      }
      //This differes in spark with mode => "DROPMALFORMED" : when a schema is 
      // provided, any rows that have values that dont conform to the schema 
      // are dropped - for ratings1, 'P2345' is missing for this reason
      queryOneColumn("SELECT PID FROM RATINGS1WITHTYPES"){ 
        _.toSeq must contain(
          str("P123"), /*str("P2345"),*/ str("P124"), str("P325")
        )
      }
      //with spark if "PERMISSIVE" mode is used when a schema is provided the
      // NULL will be there otherwise it is dropped
      /*queryOneColumn("SELECT RATING FROM RATINGS1WITHTYPES"){
        _.toSeq must contain(
          NullPrimitive()
        )
      }*/
      //Same deal here with 245.0 : spark "DROPMALFORMED" mode 
      queryOneColumn("SELECT REVIEW_CT FROM RATINGS1WITHTYPES"){
        _.toSeq must contain(
          f(50.0), /*f(245.0),*/ f(100.0), f(30.0)
        )
      }
    }

    "Load CSV files with missing values" >> {
      db.loadTable( targetTable = Some(ID("R")), sourceFile = "test/r_test/r.csv")
      val colNames: Seq[String] = db.tableSchema("R").get.map(_._1.id)
      val b = colNames(1)
      val c = colNames(2)
      queryOneColumn(s"SELECT $b FROM R"){ _.toSeq must contain(NullPrimitive()) }
      queryOneColumn(s"SELECT $c FROM R"){ _.toSeq must contain(NullPrimitive()) }
    }

    "Load CSV files with garbled data" >> {
      LoggerUtils.error("mimir.util.NonStrictCSVParser") {
        db.loadTable( targetTable = Some(ID("GARBLED")), sourceFile = "test/data/garbledRatings.csv")
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

    //TODO: translate dates correctly in spark operator translator
    "Load Dates Properly" >> {
      LoggerUtils.error("mimir.util.NonStrictCSVParser") {
       loadCSV( "EMPLOYEE", Seq(("Name","string"),("Age","int"),("JOINDATE","date"),("Salary","float"),("Married","bool")), "test/data/Employee1.csv")
      }
      
      db.query(db.sqlToRA(MimirSQL.Select("""
        SELECT JOINDATE FROM EMPLOYEE
      """)))(result => result.toList.map(_.tuple)).map{ _(0).asString } must contain(
        "2011-08-01",
        "2014-06-19",
        "2007-11-11",
        "2005-01-11"
      )
    }

  }

}