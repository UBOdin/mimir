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
    LoadCSV.handleLoadTableRaw(
      db, 
      ID("R"), 
      "test/data/serial_r.csv", 
      Some(Seq(
        ID("A") -> TInt(), 
        ID("B") -> TInt()
      )), 
      Map() 
    )
    LoadCSV.handleLoadTableRaw(
      db, 
      ID("S"), 
      "test/data/serial_s.csv", 
      Some(Seq(
        ID("B") -> TInt(), 
        ID("C") -> TInt()
      )), 
      Map() 
    )
    LoadCSV.handleLoadTableRaw(
      db, 
      ID("T"), 
      "test/data/serial_t.csv", 
      Some(Seq(
        ID("C") -> TInt(), 
        ID("D") -> TInt()
      )), 
      Map() 
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
              val query = db.sqlToRA(s)
              val serialized = Json.ofOperator(query)
              val deserialized = Json.toOperator(serialized)

              Some(deserialized must be equalTo query)
            }

            case stmt => throw new Exception("Simple test cases shouldn't have updates ($stmt)")
          }).flatten
        }
      }
    }
  }

}