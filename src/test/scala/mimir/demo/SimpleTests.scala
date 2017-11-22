package mimir.demo

import java.io.{BufferedReader, File, FileReader}

import mimir.Database
import mimir.Mimir.{conf, db}
import mimir.algebra.{PrimitiveValue, StringPrimitive, TFloat}
import org.specs2.matcher.FileMatchers
import mimir.test._
import mimir.lenses.JsonExplorerLens
import mimir.sql.JDBCBackend
import mimir.util.TimeUtils
import pattern_mixture_summarization.{Cluster, ClusteringResult, NaiveSummary, NaiveSummaryEntry}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


object SimpleTests
  extends SQLTestSpecification("tempDBDemoScript")
    with FileMatchers
{


  def time[A](description: String): ( => A) => A =
    TimeUtils.monitor(description)

  // The demo spec uses cumulative tests --- Each stage depends on the stages that
  // precede it.  The 'sequential' keyword below is necessary to prevent Specs2 from
  // automatically parallelizing testing.
  sequential

//  val jsonDB = new Database(new JDBCBackend("sqlite", "test/json.db"))
//  jsonDB.backend.open()
//  jsonDB.initializeDBForMimir();

  val productDataFile = new File("test/data/Product.sql");
  val reviewDataFiles = List(
    new File("test/data/ratings1.csv"),
    new File("test/data/jsonTest.csv")
  )

  "The Basic Demo" should {
    "Be able to open the database" >> {
      db // force the DB to be loaded
      dbFile must beAFile
    }

    "Load CSV Files" >> {

      // load all tables without TI lens
      //reviewDataFiles.foreach(db.loadTableNoTI(_))
//      db.loadTable("TWITTER",new File("test/data/twitter10kRows.txt"),false,("JSON",Seq(new StringPrimitive(""))))
//      db.loadTable("JSONTEST",new File("test/data/jsonTest.csv"),false,("JSON",Seq(new StringPrimitive(""))))
//      db.loadTable("JSONTEST2",new File("test/data/meteorite.json"),false,("JSON",Seq(new StringPrimitive(""))))
      //db.loadTable("TWITTERFULL",new File("test/data/dump-30.txt"),false,("JSON",Seq(new StringPrimitive(""))))
//      db.loadTable("JSONTEST3",new File("test/data/nasa.json"),false,("JSON",Seq(new StringPrimitive(""))))
//      db.loadTable("JSONTEST6",new File("test/data/medicine.json"),false,("JSON",Seq(new StringPrimitive(""))))
//      db.loadTable("JSONTEST4",new File("test/data/jeopardy.json"),false,("JSON",Seq(new StringPrimitive(""))))
//      db.loadTable("JSONTEST5",new File("test/data/billionaires.json"),false,("JSON",Seq(new StringPrimitive(""))))

      println("DONE LOADING")
/*

      query("SELECT JSON_EXPLORER_PROJECT(JSONCOLUMN) FROM JSONTEST;"){
        _.map{
          _(0)
        }
      }

      query("SELECT JSON_EXPLORER_MERGE(JSON_EXPLORER_PROJECT(JSONCOLUMN)) FROM JSONTEST;"){
        _.map{
          _(0)
        }
      }
*/
/*
      query("SELECT JSONCOLUMN FROM TWITTERFULL;"){
        _.foreach(println(_))
      }

      time("Query Time") {
        query("SELECT JSON_CLUSTER_PROJECT(JSONCOLUMN) FROM TWITTERFULL;") {
          _.foreach(println(_))
        }
      }
      println("Done")
*/
/*
    time("Query Time") {
      query("SELECT JSON_CLUSTER_AGG(JSON_CLUSTER_PROJECT(JSONCOLUMN)) FROM TWITTERFULL;") {
        _.foreach(println(_))
      }
    }
*/

/*
      time("Query Time") {
        query("SELECT CLUSTER_TEST(JSONCOLUMN) FROM TWITTERFULL;") {
          _.foreach(println(_))
        }
      }
*/
      // verb = how many patterns
      // error = tightness of cluster, correlations not captured
      // use margin for feature shading and error for cluster shading

//      val o1 = query("SELECT * FROM JSONTEST2"){        _.foreach(println(_))      }
//      val o2 = query("SELECT CLUSTER_TEST(JSONCOLUMN) FROM JSONTEST3"){        _.foreach(println(_))      }
//      val o3 = query("SELECT CLUSTER_TEST(JSONCOLUMN) FROM TWITTERFULL"){        _.foreach(println(_))      }
//        val o4 = query("SELECT CLUSTER_TEST(JSONCOLUMN) FROM JSONTEST6"){        _.foreach(println(_))      }
//      val o5 = query("SELECT COUNT(JSONCOLUMN) FROM JSONTEST6"){        _.foreach(println(_))      }

/*
      val dir: String = "cluster\\companies"
      val path: String = s"C:\\Users\\Will\\Documents\\GitHub\\mimir\\$dir"
      val reader = new BufferedReader(new FileReader(new File(s"$dir/schema.txt")))

      val schema: List[String] = reader.readLine().split(",").toList
      val clusterResult: ClusteringResult = new ClusteringResult(s"$path\\fvoutput.txt",s"$path\\multoutput.txt",s"$path\\hamming_labels.txt")
      val clusterList = clusterResult.getClusters.asScala.toList
      val clusterHierarchy = clusterResult.getNaiveSummaryHierarchy

      val hResult: List[(Int,Double,scala.collection.mutable.Buffer[(String,Int,Double)])] = clusterList.map((x) => {
        val clusterNumber: Int = x._1
        val clusterObject: Cluster = x._2
        val clusterError: Double = clusterObject.getError
        val clusterNaiveSummaryList: scala.collection.mutable.Buffer[(String,Int,Double)] = clusterObject.getNaiveSummary.getContent.asScala.map((ns) => {Tuple3(schema(ns.featureID),ns.occurrence,ns.marginal)})
        (clusterNumber,clusterError,clusterNaiveSummaryList)
      })

      val intersect = hResult(0)._3.intersect(hResult(1)._3)

      val result: List[(Int,Double,scala.collection.mutable.Buffer[(String,Int,Double)])] = clusterList.map((x) => {
        val clusterNumber: Int = x._1
        val clusterObject: Cluster = x._2
        val clusterError: Double = clusterObject.getError
        val clusterNaiveSummaryList: scala.collection.mutable.Buffer[(String,Int,Double)] = clusterObject.getNaiveSummary.getContent.asScala.map((ns) => {Tuple3(schema(ns.featureID),ns.occurrence,ns.marginal)})
        (clusterNumber,clusterError,clusterNaiveSummaryList)
      })
*/
      // make cluster tree instead of multiple JPanels
      // turning optional fields into required fields in union types and in lower levels
      // either for mutually exclusive columns
      // max, average
      // highest entropy distribution for a set of features
      // outline, what is the concrete problem being solved, how to break down into pieces
/*
      val res: List[(Integer,Double,NaiveSummary)] = clusterList.map((x)=> {
        val c: Cluster = x._2
        val err: Double = c.getError
        val ns = c.getNaiveSummary
        (x._1,err,ns)
      })
*/
//      val sorted = result.sortBy(_._2) // sort by error

      // List[List[(ColumnName,Marginal)]]

      val b = new mimir.util.VisualizeHTML("cluster\\twitterFull")


      println("Done")

      /*
            // Test Create Type Inference
            val createTI: String = """CREATE LENS TI1 as SELECT * FROM RATINGS1 WITH TYPE_INFERENCE(.8);"""
            update(createTI)

            // Try and select from TI lens
            query("SELECT * FROM TI1;"){
              _.map{
                _(0)
              }
            }
      */
      /*
            // JSON EXPLORER Query

            val JsonQuery =  "SELECT * FROM JSONDATA;"
            query("SELECT * FROM JSONTEST;"){
              _.map{
                _(0)
              }
            }

            val explorer = JsonExplorerLens
            explorer.create(database,"TEST1",mimir.util.SqlUtils.plainSelectStringtoOperator(database,JsonQuery), List(StringPrimitive("JSONCOLUMN").asInstanceOf[PrimitiveValue]))
      */
      /*
            query("SELECT RATING FROM RATINGS1_RAW;") {
              _.map {
                _ (0)
              }.toSeq must contain(str("4.5"), str("A3"), str("4.0"), str("6.4"))
            }

            val result = query("SELECT * FROM RATINGS1.RATINGS1;") {
              _.toSeq
            }
      */
      true
    }

    // quality score for structure
    // "Your data is great! vs your data is baadddd
    // twitter would be worse than csv for example
    // relationship between children of shape, path suffix check
    // sub-schemas may be repeated multiple times, user field may over-lap with mentions, finding sub-schemas that are repeated, can use suffixes

  }
}