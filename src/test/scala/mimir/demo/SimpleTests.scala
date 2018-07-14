package mimir.demo

import java.io._

import mimir.Database
import mimir.Mimir.{conf, db}
import mimir.algebra.{PrimitiveValue, StringPrimitive, TFloat}
import org.specs2.matcher.FileMatchers
import mimir.test._
import mimir.lenses.JsonExplorerLens
import mimir.sql.JDBCBackend
import mimir.util.TimeUtils
//import pattern_mixture_summarization.{Cluster, ClusteringResult, NaiveSummary, NaiveSummaryEntry}

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

      // small files
/*
      db.loadTable("ENRON",new File("test/data/enron.json"),false,("JSON",Seq(new StringPrimitive(""))))
      db.loadTable("COMPANIES",new File("test/data/companies.json"),false,("JSON",Seq(new StringPrimitive(""))))
      db.loadTable("METEORITE",new File("test/data/meteorite.json"),false,("JSON",Seq(new StringPrimitive(""))))
      db.loadTable("MEDICINE",new File("test/data/medicine.json"),false,("JSON",Seq(new StringPrimitive(""))))
      db.loadTable("USDA",new File("test/data/USDA.json"),false,("JSON",Seq(new StringPrimitive(""))))
*/
      // large files
//      db.loadTable("YELP",new File("test/data/yelp_dataset"),false,("JSON",Seq(new StringPrimitive(""))))
      //time("Load Time"){db.loadTable("TWITTERFULL",new File("test/data/dump-30.txt"),false,("JSON",Seq(new StringPrimitive(""))))}

     // time("TWITTERFULL"){query("SELECT JSON_EXPLORER_MERGE(JSON_EXPLORER_PROJECT(JSONCOLUMN)) FROM TWITTERFULL LIMIT 5000"){        _.foreach(println(_))      }}
      //query("SELECT COUNT(JSONCOLUMN) FROM TWITTERFULL"){        _.foreach(println(_))      }

//      println("DATA LOADED")


      // generate files for clustering
/*
      var writer: BufferedWriter = new BufferedWriter(new FileWriter("clusterOptions.txt"))
      writer.write("cluster/yelpFull")
      writer.close()
      time("YELP"){query("SELECT CLUSTER_TEST(JSONCOLUMN) FROM YELP"){        _.foreach(println(_))      }}
      query("SELECT COUNT(JSONCOLUMN) FROM YELP"){        _.foreach(println(_))      }
*/
/*
      var writer: BufferedWriter = new BufferedWriter(new FileWriter("clusterOptions.txt"))
      writer.write("cluster/enron")
      writer.close()
      time("ENRON"){query("SELECT CLUSTER_TEST(JSONCOLUMN) FROM ENRON"){        _.foreach(println(_))      }}
      query("SELECT COUNT(JSONCOLUMN) FROM ENRON"){        _.foreach(println(_))      }

      var writer = new BufferedWriter(new FileWriter("clusterOptions.txt"))
      writer.write("cluster/medicineSummary")
      writer.close()
      time("MEDICINE_SUMMARY"){query("SELECT CLUSTER_TEST(JSONCOLUMN) FROM MEDICINE"){        _.foreach(println(_))      }}
      query("SELECT COUNT(JSONCOLUMN) FROM MEDICINE"){        _.foreach(println(_))      }

      var writer = new BufferedWriter(new FileWriter("clusterOptions.txt"))
      writer.write("cluster/USDA")
      writer.close()
      time("USDA_SUMMARY"){query("SELECT CLUSTER_TEST(JSONCOLUMN) FROM USDA"){        _.foreach(println(_))      }}
      query("SELECT COUNT(JSONCOLUMN) FROM USDA"){        _.foreach(println(_))      }


      var writer = new BufferedWriter(new FileWriter("clusterOptions.txt"))
      writer.write("cluster/companies")
      writer.close()
      time("COMPANIES"){query("SELECT CLUSTER_TEST(JSONCOLUMN) FROM COMPANIES"){        _.foreach(println(_))      }}
      query("SELECT COUNT(JSONCOLUMN) FROM COMPANIES"){        _.foreach(println(_))      }

       writer = new BufferedWriter(new FileWriter("clusterOptions.txt"))
      writer.write("cluster/meteorite")
      writer.close()
      time("METEORITE"){query("SELECT CLUSTER_TEST(JSONCOLUMN) FROM METEORITE"){        _.foreach(println(_))      }}
      query("SELECT COUNT(JSONCOLUMN) FROM METEORITE"){        _.foreach(println(_))      }
*/
/*
      var writer: BufferedWriter = new BufferedWriter(new FileWriter("clusterOptions.txt"))
      writer.write("cluster/twitterFull2")
      writer.close()
      time("TWITTERFULL"){query("SELECT CLUSTER_TEST(JSONCOLUMN) FROM TWITTERFULL"){        _.foreach(println(_))      }}
      query("SELECT COUNT(JSONCOLUMN) FROM TWITTERFULL"){        _.foreach(println(_))      }
*/
/*
      var reader = new BufferedReader(new FileReader(new File("clusterOptions.txt")))
      val dir: String = reader.readLine()
      reader.close()

      val path: String = s"C:\\Users\\Will\\Documents\\GitHub\\mimir\\$dir"
      reader = new BufferedReader(new FileReader(new File(s"$dir/schema.txt")))

      new mimir.util.VisualizeHTML(dir)

      println("Done")
*/
      db.loadTable("CARL",new File("test/data/carlDataSample.out"),false,("JSON",Seq(new StringPrimitive(""))),addToDB = true, naive = false)
      var writer = new BufferedWriter(new FileWriter("clusterOptions.txt"))
      writer.write("cluster/carlData")
      writer.close()
      time("CARL_SUMMARY"){query("SELECT CLUSTER_TEST(JSONCOLUMN) FROM CARL"){        _.foreach(println(_))      }}
      //query("SELECT COUNT(JSONCOLUMN) FROM USDA"){        _.foreach(println(_))      }

      true

    }
  }
}
