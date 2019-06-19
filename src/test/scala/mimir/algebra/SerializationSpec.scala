package mimir.algebra;

import java.io._
import org.specs2.mutable._
import org.specs2.specification.core.{Fragment,Fragments}
import org.specs2.specification._

import mimir.util._
import mimir.test._
import mimir.serialization._
import mimir.parser.SQLStatement

object SqlFilesOnly extends FileFilter {

  def accept(f: File) =
  {
    f.getName().split("\\.").reverse.head.toLowerCase == "sql"
  }

}


object SerializationSpec extends SQLTestSpecification("SerializationTest") with BeforeAll {

  def beforeAll = {
    loadCSV("R", 
      Seq(
        "A" -> "int", 
        "B" -> "int"
      ), 
      "test/data/serial_r.csv"
    )
    loadCSV("S", 
      Seq(
        "B" -> "int",
        "C" -> "int"
      ), 
      "test/data/serial_s.csv"
    )
    loadCSV("T", 
      Seq(
        "C" -> "int", 
        "D" -> "int"
      ), 
      "test/data/serial_t.csv"
    )
  }

  "The Algebra Serializer" should {
    val testDirectory = new File("test/sanity/simple")

    "Pass Simple Tests" >> {
      Fragments.foreach(
        testDirectory.
          listFiles(SqlFilesOnly)
      ) { case (file:File) =>
        file.getName().split("\\.").head in {
          var i = 0 
          stmts(file).map({
            case SQLStatement(s:sparsity.statement.Select) => {
              i = i + 1;
              // println(s"$file -> parsed: $s")
              val query = db.sqlToRA(s)
              // println(s"converted: $query")
              val serialized = Json.ofOperator(query)
              // println(s"serialized: $serialized")
              val deserialized = Json.toOperator(serialized)
              // println(s"deserialized: $deserialized")

              Some(deserialized must be equalTo query)
            }

            case stmt => throw new Exception("Simple test cases shouldn't have updates ($stmt)")
          }).flatten
        }
      }
    }
  }

}