package mimir.load

import java.io.{File,FileInputStream}
import java.util.zip.GZIPInputStream
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.sys.process.Process
import scala.util.matching._
import scala.io.Source

object FileFormat 
  extends Enumeration 
  with LazyLogging
{
  type T = Value
  val Raw, GZip = Value


  def ofFile(path: String): T =
    ofFile(new File(path))
  def ofFile(path: File): T =
  {
    logger.debug(s"ofFile(${path.getName})")

    val fileExtension = 
      ("""\.([^.]+)$""".r findFirstMatchIn path.getName) match {
        case Some(m) => m.group(1)
        case None => ""
      }

    byExtension(fileExtension)
      .getOrElse { 
        byFileContents(path.toString) 
          .getOrElse(Raw)
      }
  }

  def byExtension(extension: String): Option[T] =
  {
    Some(extension.toLowerCase match {
      case "gzip" | "gz" => GZip
      case "csv" | "tsv" => Raw
      case _ => return None
    })
  }

  def byFileContents(path: String): Option[T] =
  {
    inspectFile(path.toString).map { 
      case ("ASCII text", _)           => Raw
      case ("gzip compressed data", _) => Raw
      case (x, _) => {
        logger.debug(s"Unknown format description from `file`: $x")
        return None
      }
    }
  }

  def inspectFile(path: String): Option[(String, Map[String,String])] =
  {
    val output = 
      try {
        Process(Seq("file", "-0", path)).!!
      } catch {
        case _:RuntimeException => return None
      }
    if(output == null || output == ""){ return None }
    val fields = output.split("\0(: *)?")(1).trim.split(", *")
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

  def formatAwareSource(file: File): Source =
  {
    ofFile(file) match {
      case Raw => Source.fromFile(file)
      case GZip => {
        Source.fromInputStream(
          new GZIPInputStream(
            new FileInputStream(file)
          )
        )
      }
    }
  }
}
