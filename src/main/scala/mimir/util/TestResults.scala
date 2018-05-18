package mimir.util

import java.nio.file.Files
import java.nio.file.Paths
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.io.ByteArrayOutputStream
import java.io.PrintWriter

object TestResults {
  def main(args: Array[String]) {
    println("running tests....")
    parseTestResults(args.headOption)
  }
  
  def parseTestResults(test:Option[String] = None) = {
    val sbtCmd = test match {
      case None => "test"
      case Some(x) => s"testOnly $x"
    }
    val procOutput = runCommand(Seq("/opt/local/bin/sbt",sbtCmd))._2.replaceAll("""\x1b\[[0-9;]*[a-zA-Z]""", "")
    
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