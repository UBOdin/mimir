package mimir.util

import java.io.File
import java.io.FileNotFoundException
import java.io.PrintWriter
import java.io.UnsupportedEncodingException
import java.util.ArrayList
import java.util.Iterator
import java.util.Map
import java.util.Scanner
import java.util.Set
import scala.util.control.Breaks._

import scala.collection.JavaConverters._
import com.github.wnameless.json.flattener._

/*
@param inputFile The input file name/path
@param outputFileName A string that is the desired output file's name, it will be a csv so should be name.csv
@param fileEncodingType example UTF-8
@param columnLimit used to limit the number of columns in the csv
@param rowLimit used to limit the number of rows in the csv

Basic framework for json to csv for mimir. Does some conversions for names to fit into mimir, could be converted back to original in loadCSV, i.e replaces all [ ] commas and periods
Can be extended to work for multiple files, currently one file and each new line is a new json object.

 */

class JsonToCSV() {

  def convertToCsv(inputFile:File,outputFileName:String,fileEncodingType:String,columnLimit:Int,rowLimit:Int):Unit = {
    singleFile(inputFile,outputFileName,fileEncodingType,columnLimit,rowLimit)
  }

/*  def convertToCsv(inputFile:Array[File],outputFileName:String,fileEncodingType:String,columnLimit:Int,rowLimit:Int):Unit = {
    multipleFiles(inputFile,outputFileName,fileEncodingType,columnLimit,rowLimit);
  }
*/
  def multipleFiles(inputFile:File,outputFileName:String,fileEncoding:String,columnLimit:Int,rowLimit:Int): Unit = {
    var writer:PrintWriter  = null
    try {
      writer = new PrintWriter(outputFileName, fileEncoding)
    } catch{
      case e: FileNotFoundException => e.printStackTrace()
      case e: UnsupportedEncodingException => e.printStackTrace()
    }

    var maxSchema:ArrayList[String] = new ArrayList[String]()
    var columnCounter:Int = 0
    var objectCount:Int = 0

    var files:Array[File]  = inputFile.listFiles()
    for(i <- 0 until files.length){

      var x:Scanner = null
      try{
        x = new Scanner(files(i),fileEncoding)
      }
      catch{
        case e:Exception => println("Error")
      }

      if(x == null){
        println("x is null")
      }

      while(x.hasNextLine()){
        if(objectCount > rowLimit){
          break
        }
        var nextLine:String = x.nextLine()
        if(nextLine.length() > 1){
          val keySet:Set[String] = JsonFlattener.flattenAsMap(nextLine).keySet()
          keySet.asScala.map((key:String) => {
            if(!maxSchema.contains(key)){
              maxSchema.add(key)
            }
          })
          objectCount+=1
        }

      }

      x.close()

    }

    var schemaHeader:String = ""
    maxSchema.asScala.map((sch:String)=>{
      if(columnCounter > columnLimit){
        break
      }
      var s:String = sch
      s = s.replaceAll("\\.", "_dot_")
      s = s.replaceAll(",", "_com_")
      s = s.replaceAll("\\[", "|")
      s = s.replaceAll("\\]", "|")
      schemaHeader += s + ","
      columnCounter+=1
    })

    columnCounter = 0

    writer.println(schemaHeader)

    println("Maximal Schema Generated")

    for(i <- 0 until files.length){
      var x:Scanner = null;
      try{
        x = new Scanner(files(i),fileEncoding)
      }
      catch{
        case e:Exception => println("Error")
      }
      if(x == null){
        System.out.println("x is null")
      }

      objectCount = 0;

      while(x.hasNextLine()){
        if(objectCount > rowLimit){
          break
        }
        var nextLine:String = x.nextLine()
        if(nextLine.length() > 1){
          var flatJson:Map[String, Object]  = JsonFlattener.flattenAsMap(nextLine)
          if(flatJson != null){
            var schemaIter:java.util.Iterator[String]  = maxSchema.iterator()
            var row:String = ""

            while(schemaIter.hasNext()){
              if(columnCounter > columnLimit){
                break
              }
              columnCounter+=1
              var nextKey:String = schemaIter.next()
              if(nextKey != null){
                if(flatJson.containsKey(nextKey)){
                  var value:Object = flatJson.get(nextKey)
                  if(value != null){
                    row += (value.toString()).replaceAll("'", "_singleQuote_").replaceAll(",", "_comma_") + ","
                  }
                  else{
                    row += ","
                  }
                }
                else{
                  row += ","
                }
              }
            }
            writer.println(row)
          }
          columnCounter = 0
          objectCount+=1
        }

      }

      x.close()
    }
    writer.close()

    System.out.println("Finished Converting to CSV")

  }

  def singleFile(inputFile:File,outputFileName:String,fileEncoding:String,columnLimit:Int,rowLimit:Int):Unit = {

    var maxSchema:ArrayList [String] = new ArrayList[String]()
    var checkSchema:ArrayList [String] = new ArrayList[String]()
    var columnCounter:Int = 0

    var x:Scanner = null
    try {
      x = new Scanner(inputFile, fileEncoding)
    }
    catch{
      case e:Exception => println("Error opening file")
    }
    if (x == null) {
      System.out.println("scanner is null")
    }
    var rowCount:Int = 0
    while (x.hasNextLine()) {
      if (rowCount > rowLimit) {
        break
      }
      var nextLine:String = x.nextLine()
      if (nextLine.length() > 1) {
        var keySet:Set[String]  = JsonFlattener.flattenAsMap(nextLine).keySet()
        keySet.asScala.map((key:String) => {
          //				key = key.replaceAll("\\.", "_dot_");
          if (!maxSchema.contains(key)) {
            maxSchema.add(key)
          }
        })
        rowCount += 1
      }

    }

    x.close()

    System.out.println("Maximal Schema Generated")

    try {
      x = new Scanner(inputFile, fileEncoding)
    }
    catch{
      case e:Exception => println("Error opening file")
    }
    if (x == null) {
      System.out.println("scanner is null")
    }

    var writer:PrintWriter = null
    try {
      writer = new PrintWriter(outputFileName, fileEncoding)
    }
    catch{
      case e: FileNotFoundException => e.printStackTrace()
      case e: UnsupportedEncodingException => e.printStackTrace()
    }

    var schemaHeader:String = ""
    maxSchema.asScala.map((sch:String) => {
      if (columnCounter > columnLimit) {
        break
      }
      var s = sch
      s = s.replaceAll("\\.", "_dot_")
      s = s.replaceAll(",", "_com_")
      s = s.replaceAll("\\[", "|")
      s = s.replaceAll("\\]", "|")

      if (checkSchema.contains(sch)) {
        s += "1"
        schemaHeader += s + ","
      }
      else {
        schemaHeader += s + ","
      }
      checkSchema.add(s)
      columnCounter +=1
    })

    columnCounter = 0

    writer.println(schemaHeader)

    rowCount = 0;

    while (x.hasNextLine()) {
      if (rowCount > rowLimit) {
        break
      }
      var nextLine:String = x.nextLine()
      if (nextLine.length() > 1) {
        var flatJson:Map[String, Object] = JsonFlattener.flattenAsMap(nextLine)
        if (flatJson != null) {
          var schemaIter: java.util.Iterator[String] = maxSchema.iterator()
          var row:String = ""

          while (schemaIter.hasNext()) {
            if (columnCounter > columnLimit) {
              break
            }
            columnCounter += 1
            var nextKey:String  = schemaIter.next()
            if (nextKey != null) {
              //						nextKey = nextKey.replaceAll("\\.", "_dot_");
              if (flatJson.containsKey(nextKey)) {
                var value:Object = flatJson.get(nextKey)
                if (value != null) {
                  row += (value.toString()).replaceAll("'", "_singleQuote_").replaceAll(",", "_comma_") + ","
                }
                else {
                  row += ","
                }
              }
              else {
                row += ","
              }
            }
          }
          writer.println(row)
        }
        columnCounter = 0
        rowCount += 1
      }

    }

    x.close()

    writer.close()

    println("Finished Converting to CSV")
  }
}