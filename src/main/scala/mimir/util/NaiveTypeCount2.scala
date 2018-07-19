package mimir.util

import java.io.File

import com.google.gson.Gson
import scalafx.scene.control.Tab
import scalafx.application
import scalafx.application.JFXApp
import scalafx.scene.Scene
import scalafx.scene.control.{Button, ListView, TabPane}
import scalafx.scene.layout.{FlowPane, Priority}

import vegas._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer



class NaiveTypeCount2(datasetName: String, inputFile: File, rowLimit: Int = 0, IncludeNulls: Boolean = false, naive: Boolean = false, Sample: Boolean = false, SampleLimit: Int = 10, OnlyLeafValues: Boolean = true){

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


  createSchema()

  if(Sample && false) {
    visualizeUnknownObjects()
    //visualizeTuples()
  }

  if(Sample) {
    Vegas("Country Pop").
      withData(
        countMapForVegas()
      ).
      encodeX("Name", Nom).
      encodeY("Count", Quant).
      mark(Bar).show
  }

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
      if((rowCount%100000 == 0) && added)
        println(s"$rowCount rows added, $nonJsonRowCount rows rejected so far")
      line = loadJson.getNext()
    }
    println(s"$rowCount rows added, $nonJsonRowCount rows rejected")
    println("Generating Schema")
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
      var attributeClass: Class[_ <: Object]  = null
      try {
        attributeClass = attribute._2.getClass
      } catch {
        case e: java.lang.NullPointerException => // do nothing
      }
      attributeClass match {
        case(StringClass) =>
          update2(attributeName, "String")
          returnType += s"$attributeName:String,"

        case(DoubleClass) =>
          update2(attributeName,"Double")
          returnType += s"$attributeName:Double,"

        case(BooleanClass) =>
          update2(attributeName, "Boolean")
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
          update2(attributeName, s"[$arrayType]")
          returnType += s"$attributeName:Array,"

        case(ObjectClass) =>
          val t = mapInsertType(attributeName + ".", Gson.fromJson(Gson.toJson(attributeValue), MapType))
          update2(attributeName, s"{${toObjectType(t)}}") // this is done because order shouldn't matter
          returnType += s"$attributeName:Object,"

        case(null) =>
          if(IncludeNulls) {
            update2(attributeName, "Null")
            returnType += s"$attributeName:Null,"
          }
        case _ =>
          update2(attributeName,"UnknownType")
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

  def update2(attributeName: String, attributeType: String) = {
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
    GlobalTypeMap.foreach((x) => {
      val xAttributeName = x._1
      val xTypeMap = x._2
      val objectNameHolder = scala.collection.mutable.ListBuffer[String]()
      var isArray = false
      var isObject = false
      var hypothesisType: String = null
      var hypothesisSize: Int = -1
      var variableSize = false
      var multipleType = false
//      println(xAttributeName)
//      println(xTypeMap)
//      println(calculateEntropy(xTypeMap))
      xTypeMap.foreach((y) => {
        val yTypeName = y._1
        val yTypeCount = y._2
        yTypeName.charAt(0) match {
          case ('{') => // is an object type
            isObject = true
            if(yTypeName.equals("{}")) { // is an empty array
              if(hypothesisSize == -1)
                hypothesisSize = 0
              else if (hypothesisSize != 0)
                variableSize = true
            } else {
              val nameList = yTypeName.substring(1, yTypeName.size - 1).split(',').map(x => x.split(':')(0))
              val typeList = yTypeName.substring(1, yTypeName.size - 1).split(',').map(x => x.split(':')(1))
              if (hypothesisSize == -1) {
                hypothesisSize = nameList.size
                nameList.foreach(objectNameHolder += _)
              }
              else {
                val sameSize = objectNameHolder.foldLeft(true){(same,s)=> {
                  if(same)
                    nameList.contains(s)
                  else
                    false
                }} && nameList.size == objectNameHolder.size
                if(!sameSize)
                  variableSize = true
              }
              typeList.foreach(t => {
                if(hypothesisType == null)
                  hypothesisType = t
                else if (!hypothesisType.equals(t))
                  multipleType = true
              })
            }
          case ('[') => // is an array type
            isArray = true
            if(yTypeName.equals("[]")) { // is an empty array
              if(hypothesisSize == -1)
                hypothesisSize = 0
              else if (hypothesisSize != 0)
                variableSize = true
            } else {
              val typeList = yTypeName.substring(1, yTypeName.size - 1).split(',').map(x => x.split(':')(1))
              if(hypothesisSize == -1)
                hypothesisSize = typeList.size
              else if(hypothesisSize != typeList.size)
                variableSize = true
              typeList.foreach(t => {
                if(hypothesisType == null)
                  hypothesisType = t
                else if(!hypothesisType.equals(t))
                  multipleType = true
              })
            }
          case _ => // is a leaf type, string, bool, numeric
        }
      })
      //if(xAttributeName.equals("batch") || xAttributeName.equals("SignalStrength"))
      if(isArray && isObject) // has both array and object types
        ArrayAndObject += xAttributeName
      else if(isArray && !multipleType)
        ConfirmedAsArray += xAttributeName
      else if(isArray && multipleType && variableSize) // it's children should be kept
        ArrayToObject += xAttributeName
      else if(isObject && !multipleType && variableSize) // should be an array
        ObjectToArray += xAttributeName
      else if(isObject && multipleType && variableSize)
        ConfirmedAsObject += xAttributeName
      else if(isObject && multipleType && !variableSize) // could be better represented as a tuple potentionally
        TupleType += xAttributeName
      else if(isArray && multipleType && !variableSize) // could be better represented as a tuple potentionally
        ArrayTupleType += xAttributeName
    })
    println("Confirmed Arrays: "+ConfirmedAsArray.mkString(","))
    println("Confirmed Objects: "+ConfirmedAsObject.mkString(","))
    println("Arrays to Objects: "+ArrayToObject.mkString(","))
    println("Objects to Arrays: "+ObjectToArray.mkString(",")) // retweeted_status.entities, entities, quoted_status.entities, retweeted_status.quoted_status.entities
    println("Tuples: "+TupleType.mkString(","))
    println("Array Tuples: "+ArrayTupleType.mkString(","))
    println("Had both Arrays and Objects: "+ArrayAndObject.mkString(","))
    println("Done")
  }

  // Now build the maximal schema for the parser
  def visualizeUnknownObjects() = {
    val app = new JFXApp {
      stage = new application.JFXApp.PrimaryStage{
        title = datasetName + " array like objects"
        scene = new Scene(400,600){
          val tabPane = new TabPane()
          val tabList: ListBuffer[Tab] = ObjectToArray.map(makeTab(_))
          tabPane.tabs = tabList
          root = tabPane
        }
      }
    }
    app.main(Array[String]())
  }

  def visualizeTuples() ={
    val app = new JFXApp {
      stage = new application.JFXApp.PrimaryStage{
        title = datasetName + " tuples"
        scene = new Scene(400,600){
          val tabPane = new TabPane()
          val tabList: ListBuffer[Tab] = TupleType.map(makeTabTuple(_))
          tabPane.tabs = tabList
          root = tabPane
        }
      }
    }
    app.main(Array[String]())
  }

  def makeTab(attributeName: String): Tab = {
    val tab = new Tab
    tab.text = attributeName
    val fp = new FlowPane()
    val button = new Button("Add Object to Schema")
    val button2 = new Button("Collapse Object as an Array")
    val items = SampleMap.get(attributeName).get
    val v = new ListView(items)
    v.prefHeight = 500
    v.prefWidth = 7000
    fp.getChildren.addAll(button,button2,v)
    tab.setContent(fp)
    return tab
  }

  def makeTabTuple(attributeName: String): Tab = {
    val tab = new Tab
    tab.text = attributeName
    val fp = new FlowPane()
    val button = new Button("Is a Tuple")
    val items = SampleMap.get(attributeName).get
    val v = new ListView(items)
    v.prefHeight = 500
    v.prefWidth = 7000
    fp.getChildren.addAll(button,v)
    tab.setContent(fp)
    return tab
  }

  def countMapForVegas(): Seq[Map[String,Any]] = {
    return CountMap.map(x => Map("Name" -> x._1, "Count" -> x._2)).toSeq
  }

  def calculateEntropy(m:scala.collection.mutable.Map[String,Int]): Double = {
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

}
