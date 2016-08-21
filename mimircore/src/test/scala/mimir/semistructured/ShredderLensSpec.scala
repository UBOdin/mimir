package mimir.semistructured

import java.io._
import java.util.Random

import scala.collection.JavaConversions._
import mimir._
import mimir.sql._
import mimir.algebra._
import mimir.ctables._
import mimir.exec.ResultIterator
import mimir.test._
import mimir.util._
import mimir.lenses._

object ShredderLensSpec
  extends SQLTestSpecification("shredderLensTestDB", Map("reset" -> "NO"))
{

  val testTable = "JSONDATA"
  var discala:FuncDep = null
  val testData = new File(
    "../test/data/JSONOUTPUTWIDE.csv"
    // "../test/data/Bestbuy_raw_noquote.csv"
  )
  val extractorName = "TEST_EXTRACTOR"
  var testLens:ShredderLens = null
  var writer:PrintWriter  = null;
  var writer1:PrintWriter  = null;
  try {
    writer = new PrintWriter("Results.txt", "UTF-8");
  }
  catch {
    case _ => val doNothing = null
  }
  try {
    writer1 = new PrintWriter("OUT.csv", "UTF-8");
  }
  catch {
    case _ => val doNothing = null
  }

  sequential

  "The DiScala Extractor" should {

     "be initializable" >> {
       LoadCSV.handleLoadTable(db, testTable, testData)
       val schema = db.getTableSchema(testTable).get
       discala = new FuncDep()
       discala.buildAbadi(schema, db.query(db.getTableOperator(testTable)))
       discala.entityPairMatrix must not beNull
     }

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

    "contain enough information to create a lens" >> {
      val entities = discala.entityPairList.flatMap( x => List(x._1, x._2) ).toSet.toList
      // println(entities.toString)
      entities must not beEmpty

      val primaryEntity = entities(new Random().nextInt(entities.size))
      val possibleSecondaries = 
        discala.entityPairList.flatMap({ case (a, b) => 
          if(a == primaryEntity){ Some(b) }
          else if(b == primaryEntity){ Some(a) }
          else { None }
        })

      val entityObject = (e:Integer) => (e.toInt, discala.parentTable.get(e).toList.map(_.toInt))

      val input = db.getTableOperator(testTable)
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

    "be queriable" >> {
      val results:List[List[PrimitiveValue]] = db.query(testLens.view).allRows
      val results1:List[List[PrimitiveValue]] = db.query(testLens.view).allRows
      var startQuery:Long = System.nanoTime();
      db.query(testLens.view)
      var endQuery:Long = System.nanoTime();
      println("Query TOOK: "+((endQuery - startQuery)/1000000) + " MILLISECONDS")
      writer.println("Query TOOK: "+((endQuery - startQuery)/1000000) + " MILLISECONDS")

      val s:List[(String,Type.T)] = testLens.schema()
      var sc:String = ""
      s.foreach((tup)=>{
        sc = sc+ "'"+tup._1+"',"
      })
      writer1.println(sc.substring(0,sc.size))
      results1.foreach((row)=>{
        var rw:String = ""
        row.foreach((v)=>{
          writer1.print("'"+v.toString + "',")
        })
        writer1.println(rw.substring(0,rw.size-1))
      })
      writer1.close()
      LoadCSV.handleLoadTable(db, "LENSOUTPUT", new File("OUT.csv"))
      var startQ:Long = System.nanoTime();
      db.backend.execute("SELECT * FROM LENSOUTPUT")
      var endQ:Long = System.nanoTime();
      println("Output Query TOOK: "+((endQ - startQ)/1000000) + " MILLISECONDS")
      writer.println("Output Query TOOK: "+((endQ - startQ)/1000000) + " MILLISECONDS")

      writer.close()
      results must not beEmpty
    }
  }

}
