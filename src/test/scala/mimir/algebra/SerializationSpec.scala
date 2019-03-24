package mimir.algebra;

import java.io._
import org.specs2.mutable._

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


object SerializationSpec extends SQLTestSpecification("SerializationTest") {

  def reset() =
  {
    db.getAllMatadataTables()
      .filter( !_.id.startsWith("MIMIR_") )
      .filter( !_.id.equals("SQLITE_MASTER") )
      .foreach( (x) => db.metadataBackend.update(s"DROP TABLE $x;") );
    Seq("R", "S", "T").map(table => {
      if(db.tableExists(table)) 
        db.backend.dropTable(ID(table))
    })
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
      testDirectory.
        listFiles(SqlFilesOnly).
        map((file:File) => {
          reset()

          val testName = file.getName().split("\\.").head
          var i = 0 
          stmts(file).map({
            case SQLStatement(s:sparsity.statement.Select) => {
              i = i + 1;
              val query = db.sqlToRA(s)
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