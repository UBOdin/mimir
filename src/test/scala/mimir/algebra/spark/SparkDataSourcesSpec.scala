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
import mimir.backend.QueryBackend
import mimir.util.BackupUtils
import com.github.potix2.spark.google.spreadsheets.SparkSpreadsheetService

object SparkDataSourcesSpec 
  extends SQLTestSpecification("SparkDataSourcesSpec")
  with BeforeAll
  with TestTimer
{

  def beforeAll = 
  {
    db.loadTable("test/r_test/r.csv")
  }
  
 
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
      sequential
      db.loadTable(
        sourceFile = "test/data/jsonsample.txt", 
        targetTable = Some(ID("J")), 
        force = true, 
        targetSchema = None, 
        inferTypes = Some(true), 
        detectHeaders = Some(false), 
        format = ID("json"),
        loadOptions = Map()
      )   
      
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
        val result = db.backend.execute(db.table("J"))
        db.backend.writeDataSink(result, "json", Map(), Some(outputFilename))
        val outfile = new File(outputFilename + "/_SUCCESS")
        val outputSuccess = outfile.exists() 
        BackupUtils.deleteFile(new File(outputFilename))
        outputSuccess must be equalTo true 
      }
    }
    
    
    "For Google Sheet data source" should { 
      sequential
      db.loadTable(
        sourceFile = "1-9fKx9f1OV-J2P2LtOM33jKfc5tQt4DJExSU3xaXDwU/Sheet1", 
        targetTable = Some(ID("G")), 
        force = true, 
        targetSchema = None, 
        inferTypes = Some(true), 
        detectHeaders = Some(false),
        format = ID("com.github.potix2.spark.google.spreadsheets"),
        loadOptions = Map(
          "serviceAccountId" -> "vizier@api-project-378720062738.iam.gserviceaccount.com",
          "credentialPath" -> db.backend.asInstanceOf[mimir.backend.SparkBackend].sheetCred
        )
      ) 
     
      "Be able to query from a google sheet source" >> {
        val result = query("""
          SELECT * FROM G
        """)(_.toList.map(_.tuple.toList)).toList
        
        val gschema = db.typechecker.schemaOf(db.table("G")) 
        
        gschema must contain( 
            ("JOBTITLE",TString()), ("ENDYEAR",TFloat()), ("STARTYEAR",TFloat()), ("YEARSEXPERIENCE",TFloat()), ("EMPLOYMENTTYPE",TString()), ("TIMESTAMP",TString()), ("ENDWAGE",TFloat()), ("STARTWAGE",TFloat()), ("WAGEPERIOD",TString()), ("STATE",TString()), ("GENDER",TString()), ("NEARESTLARGECITY",TString()), ("COMPANYSIZE",TFloat()))
        					
        
        result.length must be equalTo 213
        result.head.length must be equalTo 13
      }
      
      "Be able to output to google sheet" >> {
        update("""
  				CREATE LENS G_MV 
  				  AS SELECT TIMESTAMP, JOBTITLE, YEARSEXPERIENCE, STARTYEAR, STARTWAGE, ENDYEAR, ENDWAGE, NEARESTLARGECITY, STATE, COMPANYSIZE, EMPLOYMENTTYPE, GENDER, WAGEPERIOD FROM G
  				  WITH MISSING_VALUE('TIMESTAMP','JOBTITLE','YEARSEXPERIENCE','STARTYEAR','STARTWAGE','ENDYEAR','ENDWAGE','NEARESTLARGECITY','STATE','COMPANYSIZE','EMPLOYMENTTYPE','GENDER','WAGEPERIOD')
  			""")
        
        val gsheet = SparkSpreadsheetService("vizier@api-project-378720062738.iam.gserviceaccount.com", 
                    new File(db.backend.asInstanceOf[mimir.backend.SparkBackend].sheetCred))
                      .findSpreadsheet("1-9fKx9f1OV-J2P2LtOM33jKfc5tQt4DJExSU3xaXDwU")
        gsheet.deleteWorksheet("Sheet2")
        val outputFilename = "1-9fKx9f1OV-J2P2LtOM33jKfc5tQt4DJExSU3xaXDwU/Sheet2"   
        val result = db.backend.execute(db.table("G_MV"))
        db.backend.writeDataSink(result, "com.github.potix2.spark.google.spreadsheets", 
            Map("serviceAccountId" -> "vizier@api-project-378720062738.iam.gserviceaccount.com",
                "credentialPath" -> db.backend.asInstanceOf[mimir.backend.SparkBackend].sheetCred), Some(outputFilename))
        
        val outputSuccess = true
        outputSuccess must be equalTo true 
      }
    }
    
    
    "For XML data source" should { 
      sequential
      db.loadTable(
        sourceFile = "test/data/xmlsample.xml", 
        targetTable = Some(ID("X")), 
        force = true, 
        targetSchema = None, 
        inferTypes = Some(false), 
        detectHeaders = Some(false),
        format = ID("xml"),
        loadOptions = Map("rowTag" -> "book")
      ) 
      
      "Be able to query from a xml source" >> {
            
        val result = query("""
          SELECT * FROM X
        """)(_.toList.map(_.tuple.toList)).toList
        
         
        result.length must be equalTo 12
        result.head.length must be equalTo 7
      }
      
      "Be able to output to xml" >> {
        val outputFilename = "xmlsampleout"   
        val result = db.backend.execute(db.table("X"))
        db.backend.writeDataSink(result, "xml", Map(), Some(outputFilename))
        val outfile = new File(outputFilename + "/_SUCCESS")
        val outputSuccess = outfile.exists() 
        BackupUtils.deleteFile(new File(outputFilename))
        outputSuccess must be equalTo true 
      }
    }
    
    
    "For Excel data source" should { 
      sequential
      db.loadTable(
        sourceFile = "test/data/excel.xlsx",
        targetTable = Some(ID("E")), 
        force = true, 
        targetSchema = None, 
        inferTypes = Some(true), 
        detectHeaders = Some(true), 
        format = ID("com.crealytics.spark.excel"),
        loadOptions = Map("sheetName" -> "SalesOrders", // Required
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
        val result = db.backend.execute(db.table("E"))
        db.backend.writeDataSink(result, "com.crealytics.spark.excel", 
            Map("dataAddress" -> "'SalesOrders'!A1", "useHeader" -> "true"), Some(outputFilename))
        val outfile = new File(outputFilename)
        val outputSuccess = outfile.exists() 
        BackupUtils.deleteFile(outfile)
        BackupUtils.deleteFile(new File("."+outputFilename+".crc"))
        outputSuccess must be equalTo true 
      }
    }
    
    //this won't work unless you add the gpl mysql connector dependency
    /*"For mysql data source" should { 
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
        
         
        result.length must be greaterThan 0
        result.head.length must be greaterThan 0
      }
    }*/
    
    /*"For postgres data source" should { 
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
   
    "For data sources in S3" should {  
      sequential
      "Be able to stage a csv file to s3 and query from it" >> {
        db.loadTable(
          sourceFile = "test/r_test/r.csv",
          targetTable = Some(ID("STAGETOS3CSV"))
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
        db.loadTable(
          sourceFile = "s3n://mimir-test-data/test/r_test/r.csv",
          targetTable = Some(ID("S3CSV"))
        )
        
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
}