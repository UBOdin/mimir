package mimir.algebra.spark

import java.io.File
import java.nio.file.Paths
import org.specs2.specification.BeforeAll
import mimir.exec.spark.datasource.google.spreadsheet.SparkSpreadsheetService

import mimir.algebra._
import mimir.algebra.NullPrimitive
import mimir.algebra.RowIdVar
import mimir.algebra.RowIdPrimitive
import mimir.algebra.Var
import mimir.algebra.StringPrimitive
import mimir.algebra.TInt
import mimir.algebra.Function
import mimir.algebra.AggFunction
import mimir.algebra.BoolPrimitive
import mimir.data.FileFormat
import mimir.exec.spark.MimirSpark
import mimir.exec.spark.MimirSparkRuntimeUtils
import mimir.test.SQLTestSpecification
import mimir.test.TestTimer
import mimir.util.BackupUtils

object SparkDataSourcesSpec 
  extends SQLTestSpecification("SparkDataSourcesSpec")
  with BeforeAll
  with TestTimer
{

  def beforeAll = 
  {
    db.loader.loadTable("test/r_test/r.csv")
  }
  
  sequential

  "Data Sources for SparkBackend" should {
    "For CSV data source" should {
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
    }
    
    
    "For JSON data source" should {

      "Be able to load a JSON data source" >> {
        db.loader.loadTable(
          sourceFile = "test/data/jsonsample.txt", 
          targetTable = Some(ID("J")), 
          inferTypes = Some(true), 
          detectHeaders = Some(false), 
          format = FileFormat.JSON
        )   
        ok
      }
      
      "Be able to query from a json source" >> {
        val result = query("""
          SELECT * FROM J
        """)(_.toList.map(_.tuple.toList)).toList
        
         
        result must be equalTo List(
            List(BoolPrimitive(true), str("jerome@saunders.tm"), f(14.7048), str("Vanessa Nguyen"), i(1), i(56), i(40), str("Gary"), str("Conner")), 
            List(BoolPrimitive(false), str("annette@hernandez.bw"), f(11.214), str("Leo Green"), i(2), i(57), i(44), str("Neal"), str("Davies")), 
            List(BoolPrimitive(true), str("troy@mcneill.bt"), f(14.0792), str("Peter Schultz"), i(3), i(58), i(26), str("Christopher"), str("Brantley")))
      }
      "Be able to output to json" >> {
        val outputFilename = "jsonsampleout"   
        val result = db.compiler.compileToSparkWithRewrites(db.table("J"))
        MimirSparkRuntimeUtils.writeDataSink(result, "json", Map(), Some(outputFilename))
        val outfile = new File(outputFilename + "/_SUCCESS")
        val outputSuccess = outfile.exists() 
        BackupUtils.deleteFile(new File(outputFilename))
        outputSuccess must be equalTo true 
      }
    }
    
    
    "For Google Sheet data source" should { 

      "Be able to load a Google Sheets  data source" >> {
        db.loader.loadTable(
          sourceFile = "1-9fKx9f1OV-J2P2LtOM33jKfc5tQt4DJExSU3xaXDwU/Sheet1", 
          targetTable = Some(ID("G")), 
          inferTypes = Some(true), 
          detectHeaders = Some(false),
          format = FileFormat.GOOGLE_SHEETS,
          sparkOptions = Map(
            "serviceAccountId" -> "vizier@api-project-378720062738.iam.gserviceaccount.com",
            "credentialPath" -> MimirSpark.sheetCred
          )
        ) 
        ok
      }
     
      "Be able to query from a google sheet source" >> {
        val result = query("""
          SELECT * FROM G
        """)(_.toList.map(_.tuple.toList)).toList
        
        val gschema = db.typechecker.schemaOf(db.table("G")) 
        
        gschema must contain( 
            (ID("JOBTITLE"),TString()), 
            (ID("ENDYEAR"),TFloat()), 
            (ID("STARTYEAR"),TFloat()), 
            (ID("YEARSEXPERIENCE"),TFloat()), 
            (ID("EMPLOYMENTTYPE"),TString()), 
            (ID("TIMESTAMP"),TString()), 
            (ID("ENDWAGE"),TFloat()), 
            (ID("STARTWAGE"),TFloat()), 
            (ID("WAGEPERIOD"),TString()), 
            (ID("STATE"),TString()), 
            (ID("GENDER"),TString()), 
            (ID("NEARESTLARGECITY"),TString()), 
            (ID("COMPANYSIZE"),TFloat())
          )
        					
        
        result.length must be equalTo 213
        result.head.length must be equalTo 13
      }
      
      "Be able to output to google sheet" >> {
        update("""
  				CREATE LENS G_MV 
  				  AS SELECT TIMESTAMP, JOBTITLE, YEARSEXPERIENCE, STARTYEAR, STARTWAGE, ENDYEAR, ENDWAGE, NEARESTLARGECITY, STATE, COMPANYSIZE, EMPLOYMENTTYPE, GENDER, WAGEPERIOD FROM G
  				  WITH MISSING_VALUE('TIMESTAMP','JOBTITLE','YEARSEXPERIENCE','STARTYEAR','STARTWAGE','ENDYEAR','ENDWAGE','NEARESTLARGECITY','STATE','COMPANYSIZE','EMPLOYMENTTYPE','GENDER','WAGEPERIOD')
  			""")
        
        val gsheet = SparkSpreadsheetService(Some("vizier@api-project-378720062738.iam.gserviceaccount.com"), 
                    new File(MimirSpark.sheetCred))
                      .findSpreadsheet("1-9fKx9f1OV-J2P2LtOM33jKfc5tQt4DJExSU3xaXDwU")
        gsheet.deleteWorksheet("Sheet2")
        val outputFilename = "1-9fKx9f1OV-J2P2LtOM33jKfc5tQt4DJExSU3xaXDwU/Sheet2"   
        val result = db.compiler.compileToSparkWithRewrites(db.table("G_MV"))
        MimirSparkRuntimeUtils.writeDataSink(
            result, 
            "mimir.exec.spark.datasource.google.spreadsheet", 
            Map("serviceAccountId" -> "vizier@api-project-378720062738.iam.gserviceaccount.com",
                "credentialPath" -> MimirSpark.sheetCred), 
            Some(outputFilename)
          )
        
        val outputSuccess = true
        outputSuccess must be equalTo true 
      }
    }
    
    
    "For XML data source" should { 

      "Be able to load an XML data source" >> {
        db.loader.loadTable(
          sourceFile = "test/data/xmlsample.xml", 
          targetTable = Some(ID("X")), 
          inferTypes = Some(false), 
          detectHeaders = Some(false),
          format = FileFormat.XML,
          sparkOptions = Map("rowTag" -> "book")
        ) 
        ok
      }
      
      "Be able to query from a xml source" >> {
            
        val result = query("""
          SELECT * FROM X
        """)(_.toList.map(_.tuple.toList)).toList
        
         
        result.length must be equalTo 12
        result.head.length must be equalTo 7
      }
      
      "Be able to output to xml" >> {
        val outputFilename = "xmlsampleout"   
        val result = db.compiler.compileToSparkWithRewrites(db.table("X"))
        MimirSparkRuntimeUtils.writeDataSink(result, "xml", Map(), Some(outputFilename))
        val outfile = new File(outputFilename + "/_SUCCESS")
        val outputSuccess = outfile.exists() 
        BackupUtils.deleteFile(new File(outputFilename))
        outputSuccess must be equalTo true 
      }
    }
    
    
    "For Excel data source" should { 
      sequential

      "Be able to load an Excel spreadsheet data source" >> {
        db.loader.loadTable(
          sourceFile = "test/data/excel.xlsx",
          targetTable = Some(ID("E")), 
          inferTypes = Some(true), 
          detectHeaders = Some(true), 
          format = ID("com.crealytics.spark.excel"),
          sparkOptions = Map("sheetName" -> "SalesOrders", // Required
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
                            //"excerptSize" -> "10"*/ // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
          )
        ) 
        ok
      }
           
      
      "Be able to query from a excel source" >> {
        val result = query("""
          SELECT * FROM E
        """)(_.toList.map(_.tuple.toList)).toList
        
        db.typechecker.schemaOf(db.table("E")) must be equalTo 
         Seq(
          ID("ORDERDATE") -> TString(), 
          ID("REGION") -> TString(), 
          ID("REP") -> TString(), 
          ID("ITEM") -> TString(), 
          ID("UNITS") -> TInt(), 
          ID("UNIT_COST") -> TFloat(), 
          ID("TOTAL") -> TFloat()
        )
        
        result.length must be equalTo 43
        result.head.length must be equalTo 7
      }
      
      "Be able to output to excel" >> {
        val outputFilename = "xmlsampleout.xlsx"   
        val result = db.compiler.compileToSparkWithRewrites(db.table("E"))
        MimirSparkRuntimeUtils.writeDataSink(result, "com.crealytics.spark.excel", 
            Map("dataAddress" -> "'SalesOrders'!A1", "useHeader" -> "true"), Some(outputFilename))
        val outfile = new File(outputFilename)
        val outputSuccess = outfile.exists() 
        BackupUtils.deleteFile(outfile)
        BackupUtils.deleteFile(new File("."+outputFilename+".crc"))
        outputSuccess must be equalTo true 
      }
    }
    
   
    "For data sources in S3" should {  
      sequential
      "Be able to stage a csv file and query from it" >> {
        db.loader.loadTable(
          sourceFile = s"file://${Paths.get(".").toAbsolutePath()}/test/r_test/r.csv",
          targetTable = Some(ID("STAGETOS3CSV")),
          stageSourceURL = true
        )
        
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
        skipped("Needs to be rewritten to not require hostname hacks"); ko
        // db.loadTable(
        //   sourceFile = "s3n://mimir-test-data/test/r_test/r.csv",
        //   targetTable = Some(ID("S3CSV"))
        // )
        
        // val result = query("""
        //   SELECT * FROM S3CSV
        // """)(_.toList.map(_.tuple.toList)).toList
        
        // result must be equalTo List(
        //  List(i(1), i(2), i(3)), 
        //  List(i(1), i(3), i(1)), 
        //  List(i(2), NullPrimitive(), i(1)), 
        //  List(i(1), i(2), NullPrimitive()), 
        //  List(i(1), i(4), i(2)), 
        //  List(i(2), i(2), i(1)), 
        //  List(i(4), i(2), i(4))   
        // )
        
      }
    }    
    
    /*"For jdbc data sources" should {
        "Be able to query from a mysql source" >> {
        db.loader.loadTable(
           sourceFile = "jdbc:mysql://mysql-rfam-public.ebi.ac.uk:4497/Rfam",
           targetTable = Some(ID("M")), 
           inferTypes = Some(true), 
           detectHeaders = Some(true), 
           format = ID("jdbc"),
           sparkOptions = Map("url" -> "jdbc:mysql://mysql-rfam-public.ebi.ac.uk:4497/Rfam", 
            "driver" -> "com.mysql.jdbc.Driver", 
            "dbtable" -> "family", 
            "user" -> "rfamro", 
            "password" -> ""))
            
        val result = query("""
          SELECT * FROM M
        """)(_.toList.map(_.tuple.toList)).toList
        
        result.length must be equalTo 3016
        
        update("""
  				CREATE LENS MV_M
  				  AS SELECT * FROM M
  				  WITH MISSING_VALUE('TYPE')
   			""")
   			val querymv = db.table("MV_M")
        val resultmv = db.query(querymv)(_.toList.map(_.tuple.toList)).toList
        
        resultmv.length must be equalTo 3016
      }
      
      "Be able to query from a postgres source" >> {
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
      }
    }*/
    
    
    "For PDF data source" should {

      "Be able to load a PDF data source" >> {
        db.loader.loadTable(
          sourceFile = "test/data/sample.pdf", 
          targetTable = Some(ID("P")), 
          inferTypes = Some(true), 
          detectHeaders = Some(false), 
          format = FileFormat.PDF,
          sparkOptions = Map( "pages" -> "all", "gridLines" -> "true")
        )   
        ok
      }
      
      "Be able to query from a PDF source" >> {
        val result = query("""
          SELECT * FROM P
        """)(_.toList.map(_.tuple.toList)).toList
        
         
        result must contain( eachOf(
            List(str("01/04/2017"),	f(62.48),	f(62.75),	f(62.12),	f(62.3), str("21,325,140")),
            List(str("01/03/2017"),	f(62.79),	f(62.84),	f(62.125), f(62.58), str("20,655,190")),
            List(str("12/30/2016"),	f(62.96),	f(62.99),	f(62.03),	f(62.14),	str("25,575,720"))
            ))
      }
      
      "Be able to load a PDF data source with page and area" >> {
        db.loader.loadTable(
          sourceFile = "test/data/sample-area.pdf", 
          targetTable = Some(ID("PA")), 
          inferTypes = Some(true), 
          detectHeaders = Some(true), 
          format = FileFormat.PDF,
          sparkOptions = Map( "pages" -> "1", "guessArea" -> "true"/*"area" -> "104.99;379.05;380.91;469.8"*/, "gridLines" -> "true")
        )   
        ok
      }
      
      "Be able to query from a PDF source" >> {
        val result = query("""
          SELECT * FROM PA
        """)(_.toList.map(_.tuple.toList)).toList
        
         
        result must be equalTo List(
            List(i(5), str("3, 5, 4")), 
            List(i(10), str("7, 8, 6")), 
            List(i(15), str("11, 10, 12")), 
            List(i(20), str("15, 13, 14"))
            )
      }
    }
    
  }
}