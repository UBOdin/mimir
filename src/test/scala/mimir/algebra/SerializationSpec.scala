package mimir.algebra;

import java.util._
import java.io._
import org.specs2.mutable._

import mimir.util._
import mimir.test._
import mimir.serialization._

object SqlFilesOnly extends FileFilter {

  def accept(f: File) =
  {
    f.getName().split("\\.").reverse.head.toLowerCase == "sql"
  }

}


object SerializationSpec extends SQLTestSpecification("SerializationTest") {

  def reset() =
  {
    db.getAllTables()
      .filter( !_.startsWith("MIMIR_") )
      .filter( !_.equals("SQLITE_MASTER") )
      .foreach( (x) => db.metadataBackend.update(s"DROP TABLE $x;") );
  }

  "The Algebra Serializer" should {
    val testDirectory = new File("test/sanity/simple")

    "Pass Simple Tests" >> {
      testDirectory.
        listFiles(SqlFilesOnly).
        map((file:File) => {
          reset()

          val testName = file.getName().split("\\.").head
          var i = 0 
          stmts(file).map({
            case s:net.sf.jsqlparser.statement.select.Select => {
              i = i + 1;
              val query = db.sql.convert(s)
              val serialized = Json.ofOperator(query)
              val deserialized = Json.toOperator(serialized)

              Some(deserialized must be equalTo query)
            }

            case x => db.metadataBackend.update(x.toString); None
          }).flatten
        })
      true
    }
  }

}