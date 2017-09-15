package mimir.util

import java.io._
import scala.io._
import scala.sys.process._
import scala.util.matching.Regex
import com.typesafe.scalalogging.slf4j.LazyLogging

object PythonProcess 
  extends LazyLogging
{

  val JAR_PREFIX = "^jar:(.*)!(.*)$".r
  val FILE_PREFIX = "f".r

  def scriptPath: String =
  {
    val resource = getClass().getClassLoader().getResource("__main__.py")
    if(resource == null){
      throw new IOException("Python integration unsupported: __main__.py is unavailable");
    }

    var path = resource.toURI().toString()
    val prefix = "(jar|file):(.*)".r

    logger.debug(s"Base Path: ${path}")
    var done = false;
    while(!done) {
      (prefix findFirstMatchIn path) match {
        case None => done = true
        case Some(hit) => {
          path = hit.group(2)
          logger.debug(s"Prefix: '${hit.group(1)}' Path Now: ${path}")
          hit.group(1) match {
            case "jar" => {
              val splitPoint = path.lastIndexOf('!')
              path = path.substring(0, splitPoint)
              logger.debug(s"Stripping Resource Path; Path Now: ${path}")
            }
            case "file" => {
              return path
            }
          }
        }
      }
    }

    throw new IOException(s"Python integration unsupported: Unknown access method for __main__.py")
  }

  def apply(operation: String, io: ProcessIO = null): Process =
  {
    val cmd = s"python ${scriptPath} ${operation}"
    if(io == null){
      cmd.run()
    } else {
      cmd.run(io)
    }
  }

}