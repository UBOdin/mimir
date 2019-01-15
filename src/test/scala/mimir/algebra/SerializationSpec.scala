package mimir.algebra;

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
    db.getAllMatadataTables()
      .filter( !_.startsWith("MIMIR_") )
      .filter( !_.equals("SQLITE_MASTER") )
      .foreach( (x) => db.metadataBackend.update(s"DROP TABLE $x;") );
    Seq("R", "S", "T").map(table => {
      if(db.tableExists(table)) 
        db.backend.dropTable(table)
    })
    LoadCSV.handleLoadTableRaw(db, "R", Some(Seq(("A", TInt()), ("B", TInt()))), new File("test/data/serial_r.csv"), Map() );
    LoadCSV.handleLoadTableRaw(db, "S", Some(Seq(("B", TInt()), ("C", TInt()))), new File("test/data/serial_s.csv"), Map() );
    LoadCSV.handleLoadTableRaw(db, "T", Some(Seq(("C", TInt()), ("D", TInt()))), new File("test/data/serial_t.csv"), Map() );
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