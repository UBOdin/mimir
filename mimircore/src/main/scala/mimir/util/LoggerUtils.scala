package mimir.util

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.{Level,Logger};

object LoggerUtils {

  def trace[A](loggerName: String, body: () => A): A =
    enhance(loggerName, Level.TRACE, body)

  def debug[A](loggerName: String, body: () => A): A =
    enhance(loggerName, Level.DEBUG, body)

  def enhance[A](loggerName: String, level: Level, body: () => A): A =
  {
    val logger = LoggerFactory.getLogger(loggerName).asInstanceOf[Logger]
    val originalLevel = logger.getLevel();
    logger.setLevel(level)
    val ret = body()
    logger.setLevel(originalLevel)
    ret
  }



}