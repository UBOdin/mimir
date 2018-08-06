package mimir.util

import java.io.{BufferedReader, File, FileReader}

import com.google.gson.Gson

import scala.collection.mutable

import scala.collection.JavaConverters._

// generate the ground truth schema for yelp
class YelpGT(yelpInput: File, Sample: Boolean = true, IncludeNulls: Boolean = true, SampleLimit: Int = 10) {

  var input: BufferedReader = new BufferedReader(new FileReader(yelpInput))
  val StringClass = classOf[String]
  val DoubleClass = classOf[java.lang.Double]
  val BooleanClass = classOf[java.lang.Boolean]
  val ArrayClass = classOf[java.util.ArrayList[_]]
  val ObjectClass = classOf[com.google.gson.internal.LinkedTreeMap[_,_]]
  val Gson = new Gson()
  val MapType = new java.util.HashMap[String,Object]().getClass


  val listOfSchemas: mutable.ListBuffer[mutable.HashMap[String,String]] = mutable.ListBuffer[mutable.HashMap[String,String]]()
  var schema: mutable.HashMap[String,String] = mutable.HashMap[String,String]()
  val CountMap: scala.collection.mutable.HashMap[String,Int] = scala.collection.mutable.HashMap[String,Int]() // map that contains the number of times an attribute occurred
  val SampleMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.ListBuffer[String]] = scala.collection.mutable.HashMap[String,scala.collection.mutable.ListBuffer[String]]() // a list of samples for each attribute


  def run(): Unit = {
    var line = nextRow()
    while(line != null){
      addSchema("",line)
      line = nextRow()
    }
    //println(schema.mkString("\n"))
  }

  def addSchema(prefix: String, m: java.util.HashMap[String,Object]): Unit = {
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
      var attributeClass: Class[_ <: Object] = null
      try {
        attributeClass = attribute._2.getClass
      } catch {
        case e: java.lang.NullPointerException => // do nothing
      }

      attributeClass match {
        case(StringClass) =>
          if (attributeName.last != ']')
            update(attributeName, "String")
        case(DoubleClass) =>
          if (attributeName.last != ']')
            update(attributeName, "Numeric")
        case(BooleanClass) =>
          if (attributeName.last != ']')
            update(attributeName, "Boolean")

        case(ArrayClass) =>
          val attributeList = attributeValue.asInstanceOf[java.util.ArrayList[java.lang.Object]].asScala
          var arrayType = ""
          attributeList.zipWithIndex.foreach(x => {
            val arrayMap: java.util.HashMap[String, java.lang.Object] = new java.util.HashMap[String, java.lang.Object]()
            arrayMap.put(s"[${x._2}]", x._1)
            arrayType += addSchema(attributeName, arrayMap) + ","
          })
          if(!arrayType.equals(""))
            arrayType = arrayType.substring(0, arrayType.size - 1)
          update(attributeName, "Array")


        case(ObjectClass) =>
          val t = addSchema(attributeName + ".", Gson.fromJson(Gson.toJson(attributeValue), MapType))
          update(attributeName, "Object")


        case(null) =>
          if(IncludeNulls) {
            if (attributeName.last != ']')
              update(attributeName, "Null")
          }

        case _ =>
          //update(attributeName,"UnknownType")
      }
    })
  }

  def update(k: String, v: String): Unit = {
    schema.update(k,v)
  }

  def nextRow(): java.util.HashMap[String,Object] ={
    val row = input.readLine()
    if(row == null){
      return null
    } else {
      try {
        val json = Gson.fromJson(row, MapType)
        return json
      } catch {
        case e: com.google.gson.JsonSyntaxException =>
          if(!schema.isEmpty) {
            listOfSchemas += schema
            println(schema.keySet.mkString(","))
            schema = mutable.HashMap[String, String]()
          }
          return nextRow()
      }
    }
  }
}
