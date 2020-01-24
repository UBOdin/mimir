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
    loadCSV(targetTable = "R", 
      sourceFile = "test/data/serial_r.csv"
    )
    loadCSV(targetTable = "S", 
      sourceFile = "test/data/serial_s.csv"
    )
    loadCSV(targetTable = "T", 
      sourceFile = "test/data/serial_t.csv"
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
              val serialized = AlgebraJsonCodecs.ofOperator(query)
              // println(s"serialized: $serialized")
              val deserialized = AlgebraJsonCodecs.toOperator(serialized)
              // println(s"deserialized: $deserialized")

              Some(deserialized must be equalTo query)
            }
 
            case stmt => throw new Exception("Simple test cases shouldn't have updates ($stmt)")
          }).flatten
        }
      }
    }

    "Co/Dec Special Cases" >> {
      Fragments.foreach(Seq[(String, (()=> Operator))](
        "Uniform Samples" -> { () => 
          db.table("T").sampleUniformly(0.1) 
        },
        "Stratified Samples" -> { () => 
          db.table("T").stratifiedSample(
            "C", TInt(), 
            Map(IntPrimitive(1) -> 0.1), 
            caveat = Some( (ID("stuff"), "a warning") )
          )
        }
      )) { case (title: String, gen:(() => Operator)) =>
        title in {
          val raw = gen()
          val serialized = Json.ofOperator(raw)
          val deserialized = Json.toOperator(serialized)

          Seq(deserialized must be equalTo raw)
        }
      }

    }
  }

}