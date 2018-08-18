package mimir.util

import java.io._

import com.google.gson.Gson

import scala.collection.JavaConverters._

class FormatJsonFile(inputFile: File, outputName: String) {
  val inputDir = "rawJsonInput/"
  val outputDir = "cleanJsonOutput/"

  def cleanCitiStations(): Unit = {
    var input: BufferedReader = new BufferedReader(new FileReader(inputFile))
    var wholeFile = ""
    var line = input.readLine()
    while(line != null){
      wholeFile += line
      line = input.readLine()
    }
    val Gson = new Gson()
    val MapType = new java.util.HashMap[String,Object]().getClass
    val json = Gson.fromJson(wholeFile,MapType)
    val writer = new BufferedWriter(new FileWriter(outputDir+outputName))
    json.get("stationBeanList").asInstanceOf[java.util.ArrayList[_]].asScala.foreach(x => writer.write(Gson.toJson(x)+"\n"))
    writer.close()
  }

  def cleanWeatherUG(): Unit = {
    var input: BufferedReader = new BufferedReader(new FileReader(inputFile))
    var wholeFile = ""
    var line = input.readLine()
    while(line != null){
      wholeFile += line
      line = input.readLine()
    }
    val Gson = new Gson()
    val MapType = new java.util.HashMap[String,Object]().getClass
    val json = Gson.fromJson(wholeFile,MapType)
    val writer = new BufferedWriter(new FileWriter(outputDir+outputName))
    json.get("hourly_forecast").asInstanceOf[java.util.ArrayList[_]].asScala.foreach(x => writer.write(Gson.toJson(x)+"\n"))
    writer.close()
  }

}
