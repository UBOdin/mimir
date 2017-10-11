package mimir.load

import java.io.File
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.sys.process.Process
import scala.util.matching._

object FileFormat extends Enumeration with LazyLogging
{
  type T = Value
  val Raw, GZip = Value


  val GZipFileExtension = "\\.(gzip|gz)$".r
  val CSVFileExtension = "\\.(csv|tsv)$".r


  def ofFile(path: String): T =
    ofFile(new File(path))
  def ofFile(path: File): T =
  {
    logger.debug(s"ofFile(${path.getName})")
    path.getName match {

      // If the file extension tells us what we need to know, great
      case GZipFileExtension(_) => GZip
      case CSVFileExtension(_) => Raw

      // Otherwise try to invoke the UNIX `file` command
      case _ => { 
        file(path.toString) match {

          case Some(("ASCII text", _)) => Raw
          case Some(("gzip compressed data", _)) => Raw

          // And if that fails... resort to RAW
          case None => Raw
          case Some((x, _)) => {
            logger.debug(s"Unknown format description from `file`: $x")
            Raw
          }

        }
      }

    }
  }

  def file(path: String): Option[(String, Map[String,String])] =
  {
    val output = Process(Seq("file", "-0", path)).!!
    if(output == null || output == ""){ return None }
    val fields = output.split("\0")(1).trim.split(", *")
    val baseType = fields.head
    val metadata = fields.tail
                        .zipWithIndex
                        .map { case (field, idx) => { 
                          val components = field.split(": *", 2)
                          if(components.size < 2){
                            val verbValue = field.split(" +", 2)
                            if(verbValue.size < 2){
                              "field_"+idx -> field
                            } else {
                              verbValue(0) -> verbValue(1)
                            }
                          } else {
                            components(0) -> components(1)
                          }
                        }}
                        .toMap

    return Some(baseType, metadata)
  }
}
