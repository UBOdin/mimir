package mimir.util

import org.slf4j.{LoggerFactory};
import ch.qos.logback.classic.{Level, Logger};

object LoggerUtils {

  val TRACE = Level.TRACE
  val DEBUG = Level.DEBUG
  val INFO  = Level.INFO
  val WARN  = Level.WARN
  val ERROR = Level.ERROR

  def trace[A]()(body: => A) = body
  def trace[A](loggerName: String*)(body: => A): A =
    enhance(loggerName, Level.TRACE){ body } 

  def debug[A]()(body: => A) = body
  def debug[A](loggerName: String*)(body: => A): A =
    enhance(loggerName, Level.DEBUG){ body }

  def error[A]()(body: => A) = body
  def error[A](loggerName: String*)(body: => A): A =
    enhance(loggerName, Level.ERROR){ body }

  def enhance[A](loggerName: String, level: Level)(body: => A): A =
  {
    val loggerBase = LoggerFactory.getLogger(loggerName)
    if(loggerBase.isInstanceOf[Logger]){
      val logger = loggerBase.asInstanceOf[Logger]
      val originalLevel = logger.getLevel();
      logger.setLevel(level)
      val ret = body
      logger.setLevel(originalLevel)
      ret
    } else {
      loggerBase.warn(s"Unable to set logger is instance of ${loggerBase.getClass}")
      body
    }
  }

  def enhance[A](loggerName: Seq[String], level: Level)(body: => A): A =
  {
    if(loggerName.isEmpty){ body }
    else { 
      enhance(loggerName.tail, level){ 
        enhance(loggerName.head, level)(body) 
      }
    }
  }


}