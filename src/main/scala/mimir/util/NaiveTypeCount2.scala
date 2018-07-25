package mimir.util

import java.io._
import java.util

import com.google.gson.{Gson, GsonBuilder}
import scalafx.scene.control.Tab
import scalafx.application
import scalafx.application.JFXApp
import scalafx.scene.Scene
import scalafx.scene.control.{Button, ListView, TabPane}
import scalafx.scene.layout.{FlowPane, Priority}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer



class NaiveTypeCount2(datasetName: String, inputFile: File, rowLimit: Int = 0, Verbose: Boolean = false, IncludeNulls: Boolean = false, naive: Boolean = false, Sample: Boolean = false, SampleLimit: Int = 10, Stash: Boolean = false, Unstash: Boolean = true){

  val StringClass = classOf[String]
  val DoubleClass = classOf[java.lang.Double]
  val BooleanClass = classOf[java.lang.Boolean]
  val ArrayClass = classOf[java.util.ArrayList[_]]
  val ObjectClass = classOf[com.google.gson.internal.LinkedTreeMap[_,_]]
  val Gson = new Gson()
  val MapType = new java.util.HashMap[String,Object]().getClass

  val GlobalTypeMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]] = scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]]()
  val SampleMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.ListBuffer[String]] = scala.collection.mutable.HashMap[String,scala.collection.mutable.ListBuffer[String]]() // a list of samples for each attribute
  val CountMap: scala.collection.mutable.HashMap[String,Int] = scala.collection.mutable.HashMap[String,Int]() // map that contains the number of times an attribute occurred
  val rejectedRows: ListBuffer[String] = scala.collection.mutable.ListBuffer[String]()

  var loadJson: LoadJson2ElectricBoogaloo = new LoadJson2ElectricBoogaloo(inputFile, naive=naive)

  val ArrayToObject: ListBuffer[String] = ListBuffer[String]()
  val ObjectToArray: ListBuffer[String] = ListBuffer[String]()
  val ArrayAndObject: ListBuffer[String] = ListBuffer[String]() // contains both array and object types
  val ConfirmedAsArray: ListBuffer[String] = ListBuffer[String]() // confirmed more likely to be an array
  val ConfirmedAsObject: ListBuffer[String] = ListBuffer[String]() // confirmed more likely to be an object
  val TupleType: ListBuffer[String] = ListBuffer[String]() // could be better represented as a tuple
  val ArrayTupleType: ListBuffer[String] = ListBuffer[String]() // could be better represented as a tuple
  val KeyEntropyFile = new BufferedWriter(new FileWriter("keyEntropy.json"))
  val KeyEntropyList = new ListBuffer[(Double,String)]()

  if(Unstash)
    unstash()
  else
    createSchema()
  if(Stash)
    stash()


  private def createSchema() = {
    var line: String = loadJson.getNext()
    var rowCount: Int = 0
    var nonJsonRowCount: Int = 0

    while((line != null) && ((rowCount < rowLimit) || (rowLimit < 1))){
      val added = add(line)
      if(added)
        rowCount += 1
      else if(!added && !line.equals("")){
        nonJsonRowCount += 1
        rejectedRows += line
      }
      if((rowCount%100000 == 0) && added && Verbose)
        println(s"$rowCount rows added, $nonJsonRowCount rows rejected so far")
      line = loadJson.getNext()
    }
    println(s"$rowCount rows added, $nonJsonRowCount rows rejected")
    buildPlan()
  }

  // AttributeName -> (Type, Count)
  def add(row: String): Boolean = {
    try {
      if(row == null)
        return false
      if(row.equals(""))
        return false
      val m: java.util.HashMap[String, Object] = Gson.fromJson(row, MapType)
      mapInsertType("", m)
    } catch {
      case e: com.google.gson.JsonSyntaxException =>
        return false
    }
    return true
  }

  def mapInsertType(prefix: String, m: java.util.HashMap[String,Object]): String = {
    var returnType: String = ""
    m.asScala.foreach(attribute => {
      val attributeName = prefix + attribute._1.replace(",",";").replace(":",";")
      val attributeValue = attribute._2
      if(Sample){
        //if(attributeName.equals("attributes.HairSpecializesIn"))
          //if(attributeValue != null)
            //println(attributeValue.toString)
        CountMap.get(attributeName) match {
          case Some(count) =>
            CountMap.update(attributeName, count + 1)
            if(attributeValue != null) {
              if (count < SampleLimit) {
                val tm = SampleMap.get(attributeName).get
                tm += attributeValue.toString
                SampleMap.update(attributeName, tm)
              }
            } else {
              if (count < SampleLimit) {
                val tm = SampleMap.get(attributeName).get
                tm += "null"
                SampleMap.update(attributeName, tm)
              }
            }

          case None =>
            CountMap.update(attributeName, 1) // count nulls
            if(attributeValue != null) {
              SampleMap.update(attributeName, scala.collection.mutable.ListBuffer[String](attributeValue.toString))
            } else {
              SampleMap.update(attributeName, scala.collection.mutable.ListBuffer[String]("null"))
            }
        }
      }
      var attributeClass: Class[_ <: Object] = null
      try {
        attributeClass = attribute._2.getClass
      } catch {
        case e: java.lang.NullPointerException => // do nothing
      }
      attributeClass match {
        case(StringClass) =>
          update(attributeName, "String")
          returnType += s"$attributeName:String,"

        case(DoubleClass) =>
          update(attributeName,"Double")
          returnType += s"$attributeName:Double,"

        case(BooleanClass) =>
          update(attributeName, "Boolean")
          returnType += s"$attributeName:Boolean,"

        case(ArrayClass) =>
          val attributeList = attributeValue.asInstanceOf[java.util.ArrayList[java.lang.Object]].asScala
          var arrayType = ""
          attributeList.zipWithIndex.foreach(x => {
            val arrayMap: java.util.HashMap[String, java.lang.Object] = new java.util.HashMap[String, java.lang.Object]()
            arrayMap.put(s"[${x._2}]", x._1)
            arrayType += mapInsertType(attributeName, arrayMap) + ","
          })
          if(!arrayType.equals(""))
            arrayType = arrayType.substring(0, arrayType.size - 1)
          update(attributeName, s"[$arrayType]")
          returnType += s"$attributeName:Array,"

        case(ObjectClass) =>
          val t = mapInsertType(attributeName + ".", Gson.fromJson(Gson.toJson(attributeValue), MapType))
          update(attributeName, s"{${toObjectType(t)}}") // this is done because order shouldn't matter
          returnType += s"$attributeName:Object,"

        case(null) =>
          if(IncludeNulls) {
            update(attributeName, "Null")
            returnType += s"$attributeName:Null,"
          }
        case _ =>
          update(attributeName,"UnknownType")
          returnType += s"$attributeName:UnknownType,"
      }
    })
    if(returnType.equals(""))
      return ""
    else
      return returnType.substring(0,returnType.size-1) // remove the last comma for split
  }

  def toObjectType(typeString:String): String= {
    val typeArray = typeString.split(",")
    val sorted = typeArray.sortBy(f => f.split(":")(0))
    return sorted.mkString(",")
  }

  def update(attributeName: String, attributeType: String) = {
    GlobalTypeMap.get(attributeName) match {
      case Some(typeMap) =>
        typeMap.get(attributeType) match {
          case Some(count) =>
            typeMap.update(attributeType, count + 1)
          case None =>
            typeMap.update(attributeType,1)
        }
        GlobalTypeMap.update(attributeName,typeMap)
      case None => GlobalTypeMap.update(attributeName, scala.collection.mutable.HashMap(attributeType -> 1))
    }
  }

  // takes the global map that is filled with AttributeNames -> Map(Types,Count)
  // reduce wants to find if arrays and objects that have multiple types
  // - easy, just check the map size is > 1 if it contains [ or {
  // also wants to find if arrays and objects are variable size
  // - need to find all attributes of the array or object at the same path level and determine if all their counts are equal

  // with this then generate a schema and loop back over the dataset with the schema and create the feature vectors
  def buildPlan() = {

    // iterate through the map and find all { for objects and [ for all
    GlobalTypeMap.foreach((everyAttribute) => {
      val attributeName = everyAttribute._1
      val keySpace = everyAttribute._2
      val objectNameHolder = scala.collection.mutable.ListBuffer[String]()
      val isArray = keySpace.foldLeft(false){(arrFound,t) => {
        if(!arrFound) {
          t._1.charAt(0) == '['
        }
        else
          arrFound
      }}
      val isObject = keySpace.foldLeft(false){(arrFound,t) => {
        if(!arrFound)
          (t._1.charAt(0) == '{')
        else
          arrFound
      }}
      if(isArray && isObject){
        System.err.println(s"$attributeName contains both arrays and objects")
      } else if(isArray || isObject){
        val ksEntropy: Double = keySpaceEntropy(keySpace)
        val xValue = keySpace.map("\""+_._1+"\"")
        val yValue = keySpace.map(_._2)
        val objType: String = {
          if(isArray)
            "Array"
          else if(isObject)
            "Object"
          else
            "Unknown"
          }
        val tEntropy: Double = keySpace.foldLeft(0.0){(acc,y) =>
          val typeName = y._1
          val typeCount = y._2
          var localTypeEntropy: Double = 0.0
          if(!typeName.equals("[]") && !typeName.equals("{}")){
            localTypeEntropy = typeEntropy(typeName.substring(1,typeName.size-1))
          } // else empty array/object so 0.0 for entropy
          acc + localTypeEntropy
        }
        val score: Double = ksEntropy/(1.0+tEntropy)
        val jsonOutput: String = s"""{"title":"${attributeName}","keySpaceEntropy":$ksEntropy,"typeEntropy":$tEntropy,"entropyScore":$score,"type":"$objType","x":[${xValue.mkString(",")}],"y":[${yValue.mkString(",")}]}"""
        KeyEntropyList += Tuple2(score,jsonOutput)

      } else {
        // is a primitive Type i.e String, numeric, boolean
      }
    })
    //KeyEntropyList.sortBy(_._1)(Ordering[Double].reverse).foreach(t=>KeyEntropyFile.write(t._2+'\n'))
    //KeyEntropyFile.close()
    if(Verbose) {
      println("Confirmed Arrays: " + ConfirmedAsArray.mkString(","))
      println("Confirmed Objects: " + ConfirmedAsObject.mkString(","))
      println("Arrays to Objects: " + ArrayToObject.mkString(","))
      println("Objects to Arrays: " + ObjectToArray.mkString(",")) // retweeted_status.entities, entities, quoted_status.entities, retweeted_status.quoted_status.entities
      println("Tuples: " + TupleType.mkString(","))
      println("Array Tuples: " + ArrayTupleType.mkString(","))
      println("Had both Arrays and Objects: " + ArrayAndObject.mkString(","))
      println("Done")
    }
  }

  // computes the type entropy
  def typeEntropy(unpackedTypeList: String): Double = {
    val m: scala.collection.mutable.Map[String,Int] = scala.collection.mutable.Map[String,Int]()
    unpackedTypeList.split(",").map(_.split(":")(1)).foreach(x => {
      m.get(x) match {
        case Some(c) => m.update(x,c+1)
        case None => m.update(x,1)
      }
    })
    val total: Int = m.foldLeft(0){(count,x) => count + x._2}
    val entropy: Double = m.foldLeft(0.0){(ent,x) => {
      val p: Double = x._2.toDouble/total
      ent + (p*scala.math.log(p))
    }}
    if(entropy == 0.0)
      return entropy
    else
      return -1.0 * entropy
  }

  // computes the keyspace entropy
  def keySpaceEntropy(m:scala.collection.mutable.Map[String,Int]): Double = {
    val total: Int = m.foldLeft(0){(count,x) => {
      if(!x._1.equals("{}") && !x._1.equals("[]"))
        count + x._2
      else
        count
    }}
    val entropy: Double = m.foldLeft(0.0){(ent,x) => {
      if(!x._1.equals("{}") && !x._1.equals("[]")) {
        val p: Double = x._2.toDouble / total
        ent + (p * scala.math.log(p))
      }
      else
        ent
    }}
    if(entropy == 0.0)
      return entropy
    else
      return -1.0 * entropy
  }

  // retrieves the data that was stashed
  // loads KeyEntropyList, GlobalTypeMap, and SampleMap
  def unstash() = {
    val sampleInput: BufferedReader = new BufferedReader(new FileReader(s"cache/${datasetName}Sample.json"))
    val typeInput: BufferedReader = new BufferedReader(new FileReader(s"cache/${datasetName}Types.json"))
    val entropyInput: BufferedReader = new BufferedReader(new FileReader(s"cache/${datasetName}Entropy.json"))

    var sampleLine: String = sampleInput.readLine()
    while(sampleLine != null){
      val m: java.util.HashMap[String, Object] = Gson.fromJson(sampleLine, MapType)
      SampleMap += Tuple2(m.get("title").toString(),m.get("payload").asInstanceOf[java.util.ArrayList[String]].asScala.to[ListBuffer])
      sampleLine = sampleInput.readLine()
    }
    sampleInput.close()

    val typeLine = typeInput.readLine()
    val m = Gson.fromJson(typeLine,MapType)
    m.asScala.foreach(x => {
      val localMap = scala.collection.mutable.HashMap[String,Int]()
      x._2.asInstanceOf[com.google.gson.internal.LinkedTreeMap[String,Double]].asScala.foreach((y)=> localMap.update(y._1,y._2.toInt))
      GlobalTypeMap.update(x._1, localMap)
    })
    typeInput.close()

    var entropyLine: String = entropyInput.readLine()
    while(entropyLine != null){
      val split: Int = entropyLine.indexOf(',')
      KeyEntropyList += Tuple2(entropyLine.substring(0,split).toDouble,entropyLine.substring(split+1))
      entropyLine = entropyInput.readLine()
    }
    entropyInput.close()

  }

  // stashes the data structures that are the result of the computation, this is so re-running will be faster for future tests and development
  def stash() = {
    val sampleWriter = new BufferedWriter(new FileWriter(s"cache/${datasetName}Sample.json"))
    val typeWriter = new BufferedWriter(new FileWriter(s"cache/${datasetName}Types.json"))
    val entropyWriter = new BufferedWriter(new FileWriter(s"cache/${datasetName}Entropy.json"))

    if(Sample){
      SampleMap.foreach(x => sampleWriter.write(s"""{"title":"${x._1}","payload":[${x._2.map(Gson.toJson(_)).mkString(",")}]}\n"""))
      sampleWriter.flush()
      sampleWriter.close()
    }

    typeWriter.write("{"+GlobalTypeMap.map(x => {
      val localJsonTypeMap = "{"+x._2.map(y => s""""${y._1}":${y._2}""").mkString(",")+"}"
      s""""${x._1}":${localJsonTypeMap}"""
    }).mkString(",")+"}")
    typeWriter.flush()
    typeWriter.close()

    KeyEntropyList.foreach(x => entropyWriter.write(s"""${x._1},${x._2}\n"""))
    entropyWriter.flush()
    entropyWriter.close()
  }

  // visualization stuff

  def visualizeList() = {
    val app = new JFXApp {
      stage = new application.JFXApp.PrimaryStage{
        title = datasetName + " array like objects"
        scene = new Scene(400,600){
          val tabPane = new TabPane()
          val tabList: ListBuffer[Tab] = KeyEntropyList.sortBy(_._1)(Ordering[Double].reverse).map(x => makeTab(x._2))
          tabPane.tabs = tabList
          root = tabPane
        }
      }
    }
    app.main(Array[String]())
  }

  def makeTab(attributeJson: String): Tab = {
    val tab = new Tab
    val m: java.util.HashMap[String, Object] = Gson.fromJson(attributeJson, MapType)
    tab.text = m.get("title").toString()
    val fp = new FlowPane()
    val button = new Button("Add Object to Schema")
    val button2 = new Button("Collapse Object as an Array")
    val items: ListBuffer[String] = SampleMap.get(m.get("title").toString()).get
    val v = new ListView(items)
    v.prefHeight = 500
    v.prefWidth = 7000
    fp.getChildren.addAll(button,button2,v)
    tab.setContent(fp)
    return tab
  }

}