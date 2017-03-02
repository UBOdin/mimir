package mimir.semistructured

import java.io._
import java.util.{Random, Scanner}

import scala.collection.JavaConversions._
import mimir._
import mimir.sql._
import mimir.algebra._
import mimir.ctables._
import mimir.exec.ResultIterator
import mimir.util._
import mimir.lenses._
import scala.collection.JavaConverters._

object ShredderLensSpec
  extends SQLTestSpecification("shredderLensTest.db", Map("reset" -> "NO"))
{
  sequential

  val testTable = "twitterSmallMediumCleanRAW"
  val dbName = "TwitterSmall.db"
  var discala:FuncDep = null
  val testData = new File(
    "test/data/JSONOUTPUTWIDE.csv"
    // "../test/data/Bestbuy_raw_noquote.csv"
  )
  val extractorName = "TEST_EXTRACTOR"
  var testLens:ShredderLens = null
  val backend = new JDBCBackend("sqlite",dbName)
  backend.open()
  val database = new Database(dbName,backend)

//  var jtocsv:JsonToCSV = new JsonToCSV()


  "The DiScala Extractor" should {
/*
    "convert json to csv" >> {
      jtocsv.convertToCsv(new File("test/data/jsonsample.txt"),"jsonsampletocsv.csv","UTF-8",1000,100)
      try{
        var x = new Scanner(new File("jsonsampletocsv.csv"),"UTF-8")
        true
      }
      catch{
        case e:FileNotFoundException => false
//        case e:_ => {println("Error creating jsonFile"); false}
      }
    }

    "load the csv into the database" >> {
      LoadCSV.handleLoadTable(db,"JSONSAMPLE",new File("jsonsampletocsv.csv"))
      var result = db.backend.execute("Select * from JSONSAMPLE;")
//      result must not beEmpty
      true
    }
*/
     "be initializable" >> {
//       LoadCSV.handleLoadTable(db, testTable, testData)
//       println("DATA LOADED")
       val schema = database.getTableSchema(testTable).get
//       val schema = db.getTableSchema(testTable).get
//       val schema = db.getTableSchema(testTable).get // for loading already existing table from a database
       discala = new FuncDep()
//       discala.buildEntities(schema, database.backend.execute("SELECT * FROM " + testTable + ";"))
       discala.entityPairMatrix must not beNull
     }
/*
     "be serializable" >> {
       var startSerialize:Long = System.nanoTime();
       discala.serializeTo(db, extractorName)
       var endSerialize:Long = System.nanoTime();
       println("Serialize TOOK: "+((endSerialize - startSerialize)/1000000) + " MILLISECONDS")
       discala.entityPairMatrix must not beNull

       val blob1 =
         db.backend.singletonQuery(
           "SELECT data FROM "+FuncDep.BACKSTORE_TABLE_NAME+" WHERE name='"+extractorName+"'"
         )
       Runtime.getRuntime().exec(Array("cp", "databases/shredderLensTestDB", "shreddb"))
       blob1 must beAnInstanceOf[BlobPrimitive]
       blob1.asInstanceOf[BlobPrimitive].v.length must be greaterThan(0)
     }
	 
    "be deserializable" >> {
      discala = FuncDep.deserialize(db, extractorName)
      discala.entityPairMatrix must not beNull
    }
*/
    /*
    "contain enough information to create a lens" >> {
      val entities:List[Integer] = discala.entityPairList.flatMap( x => List(x._1, x._2) ).toSet.toList
      println("From Entities")
      entities.map((i) => {
        println("Entities: " + i)
      })
      entities must not beEmpty
	
      val primaryEntity:Integer = entities(0)
      val possibleSecondaries:List[Integer] =
        discala.entityPairList.asScala.flatMap({ case (a, b) =>
          if(a == primaryEntity){ Some(b) }
          else if(b == primaryEntity){ Some(a) }
          else { None }
        })

	
      val entityObject = (e:Integer) => (e.toInt, discala.parentTable.get(e).toList.map(_.toInt))
	
      val input = database.getTableOperator(testTable)
      testLens = new ShredderLens(
        "TEST_LENS",
        discala,
        entityObject(primaryEntity),
        possibleSecondaries.map( entityObject(_) ),
        input
      )

      // println(testLens.view)
      val inputSchema = input.schema
      val targetSchema = 
        (primaryEntity :: discala.parentTable.get(primaryEntity).toList).map(
          inputSchema(_)
        )
      testLens.schema must be equalTo(targetSchema)
    }
*/
    "be queriable" >> {
      var startQuery:Long = System.nanoTime();
      database.query(testLens.view).foreachRow( _ => {} )
      var endQuery:Long = System.nanoTime();
      println("Query TOOK: "+((endQuery - startQuery)/1000000) + " MILLISECONDS")

      var startInsert:Long = System.nanoTime();
      database.selectInto("LENSOUTPUT", testLens.view)
      var endInsert:Long = System.nanoTime();
      println("Insert TOOK: "+((endInsert - startInsert)/1000000) + " MILLISECONDS")
      
      // val s:List[(String,Type.T)] = testLens.schema()
      // var sc:String = ""
      // s.foreach((tup)=>{
      //   sc = sc+ "'"+tup._1+"',"
      // })
      // writer1.println(sc.substring(0,sc.size))
      // results1.foreach((row)=>{
      //   var rw:String = ""
      //   row.foreach((v)=>{
      //     writer1.print("'"+v.toString + "',")
      //   })
      //   writer1.println(rw.substring(0,rw.size-1))
      // })
      // writer1.close()
      // LoadCSV.handleLoadTable(db, "LENSOUTPUT", new File("OUT.csv"))
      var startQ:Long = System.nanoTime();
      val q = database.backend.execute("SELECT * FROM LENSOUTPUT;")
      var endQ:Long = System.nanoTime();
      println("Output Query TOOK: "+((endQ - startQ)/1000000) + " MILLISECONDS")
      true
    }
    
    "queries" >> {
      val table = database.getTableOperator("LENSOUTPUT")
      var startQ:Long = System.nanoTime();
      database.query(table).foreachRow( _ => {} )
      var endQ:Long = System.nanoTime();
      println("MAT Query TOOK: "+((endQ - startQ)/1000000) + " MILLISECONDS")
//      database.backend.execute("drop table if exists LENSOUTPUT;")
      true
    }
  }

}