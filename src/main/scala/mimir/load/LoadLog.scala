package mimir.load

import java.io.File
import scala.io.Source

import mimir.Database
import mimir.algebra._

object LoadLog 
{
  def load(
    db: Database, 
    targetTable: String, 
    sourceFile: File, 
    options: Map[String,String] = Map(), 
    metadata: Seq[(String,PrimitiveValue)] = Seq()
  ) {
    val source = Source.fromFile(sourceFile)

    val fields = Seq(
      ("LINE", TInt()),
      ("DATA", TString())
    ) ++ metadata.map { x => (x._1, x._2.getType) }
    val fieldNames = fields.map(_._1)

    // does the table need to be created
    if(!db.tableExists(targetTable)){
      db.backend.update(s"""
        CREATE TABLE $targetTable(${fields.map { x => x._1 + " " + x._2 }.mkString(", ")});
      """)
    }

    db.backend.fastUpdateBatch(s"""
        INSERT INTO $targetTable(${fieldNames.mkString(", ")}) 
          VALUES (${fieldNames.map( x => "?" ).mkString(", ")});
      """, 
      source.getLines
        .zipWithIndex
        .map { case (data, idx) => 
          Seq( 
            IntPrimitive(idx),
            StringPrimitive(data)
          ) ++ metadata.map { _._2 } 
        }
    )
  }
}