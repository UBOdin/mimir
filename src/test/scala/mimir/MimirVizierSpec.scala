package mimir

import java.io._
import scala.sys.process.Process

import sparsity.Name
import org.specs2.mutable._
import org.specs2.specification._

import mimir.algebra.ID
import mimir.backend._
import mimir.data._
import mimir.metadata._

class MimirVizierSpec 
  extends Specification
  with BeforeAll
  with AfterAll
{
  val dataDirectoryPath = "MimirVizierSpec"
  val dataDirectory = new File (dataDirectoryPath)
  
  def afterAll = {
    mimir.util.BackupUtils.deleteFile(dataDirectory)
  }
  
  def beforeAll = {
    if(!dataDirectory.exists())
      dataDirectory.mkdir()
    
    val dbFileName = dataDirectoryPath+"/MimirVizierSpec.db"
    val dbFile = new File (dbFileName)
    dbFile.deleteOnExit();
    
    val args = Seq(
      "--dataDirectory", dataDirectoryPath, "--db", dbFileName
    )
    val conf = new MimirConfig(args);
    val database = conf.dbname().split("[\\\\/]").last.replaceAll("\\..*", "")
    MimirVizier.db = new Database(new JDBCMetadataBackend(conf.metadataBackend(), conf.dbname()))
    // MimirVizier.db.metadata.open()
    // MimirVizier.db.backend.open(MimirVizier.db)
    // val otherExcludeFuncs = Seq("NOT","AND","!","%","&","*","+","-","/","<","<=","<=>","=","==",">",">=","^","|","OR").map { ID(_) }
    // sback.registerSparkFunctions(
    //   MimirVizier.db.functions.functionPrototypes.map(el => el._1).toSeq ++ otherExcludeFuncs , MimirVizier.db)
    // sback.registerSparkAggregates(MimirVizier.db.aggregates.prototypes.map(el => el._1).toSeq, MimirVizier.db.aggregates)
    // MimirVizier.vizierdb.sparkSession = sback.sparkSql.sparkSession
    // MimirVizier.db.open(skipBackend = true)

    if(!MimirVizier.db.catalog.tableExists(Name("CPUSPEED"))){
      MimirVizier.db.loader.loadTable(
        "test/data/CPUSpeed.csv", 
        Some(ID("CPUSPEED")),
        format = FileFormat.CSV
      )
    }
    if(!MimirVizier.db.catalog.tableExists(Name("PICK"))){
      MimirVizier.db.loader.loadTable(
        "test/data/pick.csv", 
        Some(ID("PICK")),
        format = FileFormat.CSV
      )
    }
  }

  "MimirVizier" should {
    
    "be set up properly" >> { 
      MimirVizier.db.catalog.tableExists(Name("CPUSPEED")) must beTrue
      MimirVizier.db.catalog.tableExists(Name("NOT_A_TABLE")) must beFalse

      val schema = MimirVizier.db.catalog.tableSchema(Name("CPUSPEED"))
      schema must not be(None) 
      schema.get.map { _._1 } must contain(ID("BUS_SPEED_MHZ"))
    }

    "create missing value lenses properly" >> { 
      if(MimirVizier.db.tableExists("CPUSPEED_MISSING")){
        MimirVizier.db.lenses.drop(ID("CPUSPEED_MISSING"))
      }
      MimirVizier.db.tableExists("CPUSPEED_MISSING") must beFalse
      val response = MimirVizier.createLens(
        "CPUSPEED",
        Seq("BUS_SPEED_MHZ"),
        "MISSING_VALUE",
        false,
        false,
        Some("MISSING_CPUSPEED_BUS_SPEED_MHZ")
      )
      MimirVizier.db.query(
        MimirVizier.db.table(response.lensName).limit(1)
      ) { response => response.toSeq } must not beEmpty
      
      if(MimirVizier.db.tableExists("PICK_MISSING")){
        MimirVizier.db.lenses.drop(ID("PICK_MISSING"))
      }
      MimirVizier.db.tableExists("PICK_MISSING") must beFalse
      val presponse = MimirVizier.createLens(
        "PICK",
        Seq("B"),
        "MISSING_VALUE",
        false,
        false,
        Some("MISSING_PICK_B")
      )
      MimirVizier.vistrailsQueryMimir(s"SELECT * FROM ${presponse.lensName}", true, false).colTaint must be equalTo Seq(
          Seq(true,true,false),
          Seq(true,true,true),
          Seq(true,true,true),
          Seq(true,true,true))
    }

  }
}