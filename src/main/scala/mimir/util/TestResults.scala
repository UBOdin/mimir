package mimir.util

import java.nio.file.Files
import java.nio.file.Paths
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.io.ByteArrayOutputStream
import java.io.PrintWriter
import org.rogach.scallop.ScallopConf

object TestResults {
  def main(args: Array[String]) {
    val config = new TestResultConfig(args)
    println("running tests....")
    parseTestResults(config.sbtPath(),config.sbtCmd())
  }
  
  def parseTestResults(sbtPath:String = "/opt/local/bin/sbt", sbtCmd:String = "test") = {
    val procOutput = runCommand(Seq(sbtPath,sbtCmd))._2.replaceAll("""\x1b\[[0-9;]*[a-zA-Z]""", "")
    
    val pattern = """(?m)^.*\[info\] Total.*$|^.*\[info\] Finished.*$|^.*\[info\] [\d]+ examp.*$""".r
    
    val header = "test_name,seconds,examples,expectations,failures,errors,skipped\n"
    
    val pattern2 = """\[info\] Total for specification (\w+)\s+\[info\] Finished in (.+)\R\[info\] (.+)\R""".r
    val pattern3 = """([a-zA-Z]+): (?:(\d+) minutes? )?(?:(\d+) seconds?[,:] )?(?:(\d+) ms[,:] )?(\d+) examples?, (?:(\d+) expectations?, )?(\d+) failures?, (\d+) errors?(?:, (\d+) skipped)?""".r
    val string = pattern2.findAllMatchIn(procOutput).map(mat => s"${mat.group(1)}: ${mat.group(2)}: ${mat.group(3)}")
      .map(nline => nline match {
        case pattern3(test_name,minutes,seconds,ms,examples,expectations,failures,errors,skipped) => {
          val allseconds = (minutes match {
            case "" => 0
            case null => 0
            case x => x.toInt*60
          }) + (seconds match {
            case "" => 0
            case null => 0
            case x => x.toInt
          }) +  (ms match {
            case "" => 0.0
            case null => 0.0
            case x => x.toDouble/1000.0
          })
          s"$test_name,$allseconds,$examples,$expectations,$failures,$errors,$skipped"
        }
      }).mkString("\n")
    
    val outStr = header + string
      
    println(outStr)
    Files.write(Paths.get("test_output.csv"), outStr.getBytes(StandardCharsets.UTF_8))
  }
  
  import sys.process._
  def runCommand(cmd: Seq[String]): (Int, String, String) = {
    val stdoutStream = new ByteArrayOutputStream
    val stderrStream = new ByteArrayOutputStream
    val stdoutWriter = new PrintWriter(stdoutStream)
    val stderrWriter = new PrintWriter(stderrStream)
    val exitValue = cmd.!(ProcessLogger(stdoutWriter.println, stderrWriter.println))
    stdoutWriter.close()
    stderrWriter.close()
    (exitValue, stdoutStream.toString, stderrStream.toString)
  }
  
  
}

class TestResultConfig(arguments: Seq[String]) extends ScallopConf(arguments)
{
  val experimental = opt[List[String]]("X", default = Some(List[String]()))
  val sparkHost = opt[String]("sparkHost", descr = "The IP or hostname of the spark master",
    default = Some("spark-master.local"))
  val sparkPort = opt[String]("sparkPort", descr = "The port of the spark master",
    default = Some("7077"))
  val sbtPath = opt[String]("sbtPath", descr = "The path to sbt binary",
    default = Some("/opt/local/bin/sbt"))
  val sbtCmd = opt[String]("sbtCmd", descr = "The sbt command to run",
    default = Some("test"))
}