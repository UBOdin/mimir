package mimir.exec.spark.datasource.pdf

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import technology.tabula.CommandLineApp

class PDFTableExtractor {
  
  private def csvFromCommandLineArgs(args: Array[String]): String = {
    val parser: CommandLineParser = new DefaultParser()
    val cmd: CommandLine = parser.parse(CommandLineApp.buildOptions(), args)
    val stringBuilder: java.lang.StringBuilder = new java.lang.StringBuilder()
    new CommandLineApp(stringBuilder, cmd).extractTables(cmd)
    stringBuilder.toString
  }
  
  def defaultExtract(pdfFile:String, pages:String="all", area:Option[String]=None, outFile:Option[String]=None, hasGridLines:Boolean=false, guessArea:Boolean=true) = {
    csvFromCommandLineArgs(Array[String](
                pdfFile,
                "-p", pages) 
                ++ (if(guessArea) Array("-g") 
                    else Array[String]()) 
                ++ (if(hasGridLines) Array("-l") 
                    else Array("-t")) 
                ++ (area match { 
                      case None => Array[String]() 
                      case Some(areaStr) => Array("-a", areaStr.split(";").mkString(","))
                    })
                ++ Array[String]( "-f",
                "CSV", "-o", (outFile match {
                  case Some(outName) => outName
                  case _ => pdfFile+".csv"})))
  }
}