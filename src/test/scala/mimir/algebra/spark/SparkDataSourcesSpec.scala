package mimir.algebra.spark

import mimir.algebra._
import org.specs2.specification.BeforeAll
import mimir.test.SQLTestSpecification
import mimir.algebra.NullPrimitive
import mimir.algebra.RowIdVar
import mimir.algebra.RowIdPrimitive
import mimir.algebra.Var
import mimir.algebra.StringPrimitive
import mimir.algebra.TInt
import java.io.File
import mimir.algebra.Function
import mimir.algebra.AggFunction
import mimir.util.LoadJDBC
import mimir.algebra.BoolPrimitive
import mimir.test.TestTimer

object SparkDataSourcesSpec 
  extends SQLTestSpecification("SparkDataSourcesSpec")
  with BeforeAll
  with TestTimer
{

  def beforeAll = 
  {
    db.loadTable("test/r_test/r.csv")
  }
  
  "Spark" should {
    sequential
    "Be able to query from a CSV source" >> {
      val result = query("""
        SELECT * FROM R
      """)(_.toList.map(_.tuple))
      
      result must be equalTo List(
       List(i(1), i(2), i(3)), 
       List(i(1), i(3), i(1)), 
       List(i(2), NullPrimitive(), i(1)), 
       List(i(1), i(2), NullPrimitive()), 
       List(i(1), i(4), i(2)), 
       List(i(2), i(2), i(1)), 
       List(i(4), i(2), i(4))   
      )
    }
    
    "Be able to query from a json source" >> {
      db.loadTable("J", new File("test/data/jsonsample.txt"), true, None, true, false, Map(), "json" ) 
          
      val result = query("""
        SELECT * FROM J
      """)(_.toList.map(_.tuple.toList)).toList
      
       
      result must be equalTo List(
          List(BoolPrimitive(true), str("jerome@saunders.tm"), f(14.7048), str("Vanessa Nguyen"), i(1), i(56), i(40), str("Gary"), str("Conner")), 
          List(BoolPrimitive(false), str("annette@hernandez.bw"), f(11.214), str("Leo Green"), i(2), i(57), i(44), str("Neal"), str("Davies")), 
          List(BoolPrimitive(true), str("troy@mcneill.bt"), f(14.0792), str("Peter Schultz"), i(3), i(58), i(26), str("Christopher"), str("Brantley")))
    }
    
    "Be able to query from a xml source" >> {
      db.loadTable("X", new File("test/data/xmlsample.xml"), true, None, false, false, Map("rowTag" ->	"book"), "xml" ) 
          
      val result = query("""
        SELECT * FROM X
      """)(_.toList.map(_.tuple.toList)).toList
      
       
      result.length must be equalTo 12
      result.head.length must be equalTo 7
    }
    
    "Be able to query from a excel source" >> {
      db.loadTable("E", new File("test/data/excel.xlsx"), true, None, true, true, 
                    Map("sheetName" -> "SalesOrders", // Required
                        "dataAddress" -> "'SalesOrders'!A1",
                        "useHeader" -> "false", // Required
                        "treatEmptyValuesAsNulls" -> "true", // Optional, default: true
                        //"inferSchema" -> "true", // Optional, default: false
                        //"addColorColumns" -> "true", // Optional, default: false
                        "startColumn" -> "0", // Optional, default: 0
                        "endColumn" -> "6"/*, // Optional, default: Int.MaxValue
                        "dateFormat" -> "M/d/yyyy", // Optional, default: yyyy-mm-dd
                        "timestampFormat" -> "MM-dd-yyyy hh:mm:ss", // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
                        //"maxRowsInMemory" -> "20", // Optional, default None. If set, uses a streaming reader which can help with big files
                        //"excerptSize" -> "10"*/), // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
                    "com.crealytics.spark.excel" ) 
         
    
      val result = query("""
        SELECT * FROM E
      """)(_.toList.map(_.tuple.toList)).toList
      
      db.typechecker.schemaOf(db.table("E")) must be equalTo 
       Seq(("ORDERDATE",TString()), ("REGION",TString()), ("REP",TString()), 
           ("ITEM",TString()), ("UNITS",TInt()), ("UNIT_COST",TFloat()), ("TOTAL",TFloat()))
      
      result.length must be equalTo 43
      result.head.length must be equalTo 7
    }
    
    "Be able to query from a mysql source" >> {
      db.loadTable("M", new File("jdbc:mysql://mysql-rfam-public.ebi.ac.uk/Rfam"), true, None, true, false, 
                    Map("url" -> "jdbc:mysql://mysql-rfam-public.ebi.ac.uk:4497/Rfam", 
                              "driver" -> "com.mysql.jdbc.Driver", 
                              "dbtable" -> "family", 
                              "user" -> "rfamro"/*, 
                              "password" -> ""*/), "jdbc")
      
          
      val result = query("""
        SELECT * FROM M
      """)(_.toList.map(_.tuple.toList)).toList
      
       
      result .length must be equalTo 2791
      result.head.length must be equalTo 34
    }
    
    /*"Be able to query from a postgres source" >> {
      LoadJDBC.handleLoadTableRaw(db, "P", 
        Map("url" -> "jdbc:postgresql://128.205.71.102:5432/mimirdb", 
          "driver" -> "org.postgresql.Driver", 
          "dbtable" -> "mimir_spark", 
          "user" -> "mimir", 
          "password" -> "mimir01"))
          
      val result = query("""
        SELECT * FROM P
      """)(_.toList.map(_.tuple.toList)).toList
      
      result must be equalTo List(
          List(i(1), NullPrimitive(), i(100)), 
          List(i(2), i(4), i(104)), 
          List(i(3), i(4), i(118)), 
          List(i(4), i(5), NullPrimitive()), 
          List(i(5), i(4), i(50)))
    }*/
    
    /*"Be able to query from a json source" >> {
      db.backend.readDataSource("J", "json", Map(), None, Some("test/data/jsonsample.txt")) 
          
      val result = query("""
        SELECT * FROM J
      """)(_.toList.map(_.tuple.toList)).toList
      
      result must be equalTo List(
          List(BoolPrimitive(true), str("jerome@saunders.tm"), f(14.7048), str("Vanessa Nguyen"), i(1), i(56), i(40), str("Gary"), str("Conner")), 
          List(BoolPrimitive(false), str("annette@hernandez.bw"), f(11.214), str("Leo Green"), i(2), i(57), i(44), str("Neal"), str("Davies")), 
          List(BoolPrimitive(true), str("troy@mcneill.bt"), f(14.0792), str("Peter Schultz"), i(3), i(58), i(26), str("Christopher"), str("Brantley")))
    }*/
    
    
    "Be able to stage a file to s3 and query from it" >> {
      db.loadTable("STAGETOS3CSV", "test/r_test/r.csv")
      
      val result = query("""
        SELECT * FROM STAGETOS3CSV
      """)(_.toList.map(_.tuple.toList)).toList
      
      result must be equalTo List(
       List(i(1), i(2), i(3)), 
       List(i(1), i(3), i(1)), 
       List(i(2), NullPrimitive(), i(1)), 
       List(i(1), i(2), NullPrimitive()), 
       List(i(1), i(4), i(2)), 
       List(i(2), i(2), i(1)), 
       List(i(4), i(2), i(4))   
      )
      
    }
    
    
    "Be able to query from a csv source already in s3" >> {
      db.loadTable("S3CSV", "s3n://mimir-test-data/test/r_test/r.csv")
      
      val result = query("""
        SELECT * FROM S3CSV
      """)(_.toList.map(_.tuple.toList)).toList
      
      result must be equalTo List(
       List(i(1), i(2), i(3)), 
       List(i(1), i(3), i(1)), 
       List(i(2), NullPrimitive(), i(1)), 
       List(i(1), i(2), NullPrimitive()), 
       List(i(1), i(4), i(2)), 
       List(i(2), i(2), i(1)), 
       List(i(4), i(2), i(4))   
      )
      
    }
    
    
    
    
  }
}