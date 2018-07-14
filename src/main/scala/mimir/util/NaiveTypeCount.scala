package mimir.util

import java.io._
import java.util
import java.util.Set

import com.github.wnameless.json.flattener.JsonFlattener
import com.google.gson.Gson

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

class NaiveTypeCount {

  // ColumnName -> Map[Type,Count]
  private val mainMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]] = new scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]]()
  private val countNulls = false
  private val checkTypeValues = true // checks the values

  val search = false
  val searchList = ListBuffer("diffs")

  // shreds json row and adds it to the mainMap
  // take in row, pull out schema and type, only checking arrays down one layer
  def add(row: String): Boolean = {
    // first update all the types, in the keyspace, then update all the objects
    try {
      val jsonMap: java.util.Map[String, AnyRef] = JsonFlattener.flattenAsMap(row)

      val keySet = jsonMap.keySet().toArray()

      // help me sample attributes
      if(search)
        find(searchList,keySet,jsonMap)

      keySet.foreach((key) => {
        val attribute: String = key.toString()
        val attributeType: String = getType(jsonMap.get(attribute))
        if(!attributeType.equals("Empty")) {
          if((attributeType.equals("Null") && countNulls) || !attributeType.equals("Null"))
            update(attribute,attributeType)
        }
      })

      addJsonObjectList(keySet, jsonMap)

      true
    } catch {
      case e : Exception  => false
    }
  }

  def find(list: ListBuffer[String], keySet: Array[AnyRef], jsonMap: java.util.Map[String, AnyRef]) = {
    list.foreach((f) => {
      keySet.foreach((s) => {
        if(s.toString.contains(f))
          println(s.toString + " : " + jsonMap.get(s.toString).toString)
      })
    })
  }

  // updates the count in mainMap, inserts if doesn't exist
  def update(columnName: String, typeName: String) = {
    mainMap.get(columnName) match {
      case Some(typeMap) =>
        typeMap.get(typeName) match {
          case Some(count) => typeMap.update(typeName, count + 1)
          case None =>
            typeMap.update(typeName,1)
        }
        mainMap.update(columnName,typeMap)
      case None => mainMap.update(columnName, scala.collection.mutable.HashMap(typeName -> 1))
    }
  }

  def addJsonObjectList(keys: Array[AnyRef], jsonMap: java.util.Map[String, AnyRef]) = {
    val objectList = ListBuffer[String]()
    keys.foreach((key) => {
      var currentObj = ""
      key.toString.split("\\.").foreach((o) => {
        currentObj += o
        if(!objectList.contains(currentObj) && !keys.contains(currentObj)) // checks to make sure that it's not a leaf
          objectList += currentObj
        currentObj += "."
      })
    })
    objectList.foreach((k) => {
      update(k.toString(),getObjectChild(k, jsonMap, objectList))
    })
  }

  def getObjectChild(parentObject: String, jsonMap: java.util.Map[String, AnyRef], objectList: ListBuffer[String]): String = {
    val objectMap: scala.collection.mutable.HashMap[String,String] = scala.collection.mutable.HashMap[String,String]()
    jsonMap.keySet().toArray.foreach((key) => { // for each key check to see if parent object is it's parent object
      if(key.toString.contains(parentObject)){ // now check to make sure that the parent object doesn't just happen to be part of the path and is really it's parent and is only 1 deep
        val parentSplit = parentObject.split("\\.")
        val hypSplit = key.toString.split("\\.")
        if((hypSplit.size - parentSplit.size) == 1 && parentSplit.size >= 1 && hypSplit.size >= 2){
          var equal = true
          var i: Int = 0
          for(i <- 0 until parentSplit.size){
            if(parentSplit(i) != hypSplit(i))
              equal = false
          }
          if(equal){
            val attributeType: String = getType(jsonMap.get(key.toString))
              if(!attributeType.equals("Empty")) {
                if((attributeType.equals("Null") && countNulls) || !attributeType.equals("Null"))
                  objectMap.put(key.toString,attributeType)
              }
          }
        }
      }
    })

    objectList.foreach((key) => { // for each key check to see if parent object is it's parent object
      if(key.contains(parentObject)){ // now check to make sure that the parent object doesn't just happen to be part of the path and is really it's parent and is only 1 deep
        val parentSplit = parentObject.split("\\.")
        val hypSplit = key.split("\\.")
        if((hypSplit.size - parentSplit.size) == 1 && parentSplit.size >= 1 && hypSplit.size >= 2){
          var equal = true
          var i: Int = 0
          for(i <- 0 until parentSplit.size){
            if(parentSplit(i) != hypSplit(i))
              equal = false
          }
          if(equal)
            objectMap.put(key,"Object")
        }
      }
    })
    val g = new Gson()
    val json = g.toJson(objectMap.asJava)
    return json
  }

  def getType(s: AnyRef): String = {
    var attributeType: Class[_ <: AnyRef] = null
    try {
      attributeType = s.getClass
    }
    catch {
      case e : Exception  =>
    }

    if(attributeType == classOf[java.lang.String]){
      if(checkTypeValues && (s.toString.toLowerCase().equals("") || s.toString.toLowerCase().equals("false") || s.toString.toLowerCase().equals("null")))
        return "Empty"
      else
        return "String"
    } else if (attributeType == classOf[java.lang.Boolean]){
      if(checkTypeValues && s.asInstanceOf[Boolean] == false)
        return "Empty"
      else
        return "Boolean"
    }
    else if (attributeType == classOf[java.math.BigDecimal]){
      return "Numeric"
    } else if (attributeType == null){
      return "Null"
    } else {
        //return "UNKNOWN TYPE"
        return "String"
    }
  }


  private val multiFixed = new ListBuffer[String]()
  private val multiNotFixed = new ListBuffer[String]()
  private val notMultiFixed = new ListBuffer[String]()
  private val notMultiNotFixed = new ListBuffer[String]()
  private val badApples = new ListBuffer[String]()
  // Path -> Map(Type+Size,Count)
  private val arrayCandidates = new scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]]()
  // after all added call report to get the output
  def report() = {
    val mainMapKeys = mainMap.keySet
    mainMap.foreach(t => { // for each member of
      val columnName: String = t._1
      val typeMap: scala.collection.mutable.HashMap[String,Int] = t._2
      if(columnName.contains('[')){
        val path: String = columnName.substring(0,columnName.lastIndexOf('['))
        val index: String = columnName.substring(columnName.lastIndexOf('[')+1,columnName.lastIndexOf(']'))
        arrayCandidates.get(path) match {
          case Some(arrayTypeMap) =>
            arrayTypeMap.get(typeMap.keySet.toList(0)) match {
              case Some(count) =>
                arrayTypeMap.update(typeMap.keySet.toList(0),(count + typeMap.getOrElse(typeMap.keySet.toList(0),1)))
                arrayCandidates.update(path,arrayTypeMap)
              case None =>
                arrayTypeMap.update(typeMap.keySet.toList(0), typeMap.getOrElse(typeMap.keySet.toList(0),1))
                arrayCandidates.update(path,arrayTypeMap)
            }
            arrayTypeMap.get("Count:"+index) match {
              case Some(count) =>
                arrayTypeMap.update("Count:"+index,(count + typeMap.getOrElse(typeMap.keySet.toList(0),1)))
                arrayCandidates.update(path,arrayTypeMap)
              case None =>
                arrayTypeMap.update("Count:"+index, typeMap.getOrElse(typeMap.keySet.toList(0),1))
                arrayCandidates.update(path,arrayTypeMap)
            }
          case None =>
            if(typeMap.keySet.size > 1)
              badApples += path
            arrayCandidates.update(path,scala.collection.mutable.HashMap[String,Int](typeMap.keySet.toList(0) -> typeMap.getOrElse(typeMap.keySet.toList(0),1), "Count:"+index -> typeMap.getOrElse(typeMap.keySet.toList(0),1)))
        }
      } else if(isObject(typeMap)) {
        //arrayCandidates.update(path,)
      }
    })
    // check every element has the same size
    arrayCandidates.foreach((t1) => {
      val colName = t1._1
      val tMap = t1._2
      var fixedSize = -1
      var fixedSizeBool = true
      var numberOfTypes = 0
      var multipleFound = false
      var arraySize = 0
      tMap.foreach((t2) => {
        val name = t2._1
        val count = t2._2
        if(name.contains("Count") && fixedSize == -1) // first time
          fixedSize = count
        else if(name.contains("Count")){
          if(fixedSize != count) {
            fixedSizeBool = false
            multipleFound = true
          }
          else
            multipleFound = true
        } else {
          numberOfTypes += 1
        }
        arraySize += 1
      })
      if(multipleFound && !fixedSizeBool && !badApples.contains(colName) && numberOfTypes == 1)
        multiNotFixed += colName +":"+arraySize.toString
      else if(multipleFound && fixedSizeBool && !badApples.contains(colName) && numberOfTypes > 1)
        multiFixed += colName +":"+arraySize.toString
      else if(!multipleFound && !fixedSizeBool && !badApples.contains(colName) && numberOfTypes > 1)
        notMultiNotFixed += colName +":"+arraySize.toString
      else if(!multipleFound && fixedSizeBool && !badApples.contains(colName) && numberOfTypes > 1)
        notMultiFixed += colName +":"+arraySize.toString
    })
    println("Multiple Types, Not Fixed Size: " + multiNotFixed)
    println("Multiple Types, Fixed Size: " + multiFixed)
    println("Single Type, Not Fixed Size: " + notMultiNotFixed)
    println("Single Type, Fixed Size: " + notMultiFixed)
    println("Bad Apples: " + badApples)
  }

  def isObject(typeMap: scala.collection.mutable.HashMap[String, Int]): Boolean = {
    val baseCase = false
    typeMap.foreach( v => {
      val t = v._1
      if(t.startsWith("{")){ // found at least one object instance so return true
        return true
      }
    })
    return baseCase
  }

  val IncludeNulls = false // if true then include nulls in the output as a type

  val Sample = true
  val SampleLimit = 10
  val SampleValues: scala.collection.mutable.Map[String,Int] = scala.collection.mutable.Map("hours" -> 0)
  //val SampleValues: scala.collection.mutable.Map[String,Int] = scala.collection.mutable.Map("attributes.HairSpecializesIn" -> 0,"attributes.BusinessParking"->0,"attributes.Ambience"->0,"attributes"->0,"categories"->0,"time"->0,"time.Monday"->0)
  //val SampleValues: scala.collection.mutable.Map[String,Int] = scala.collection.mutable.Map("cms_prescription_counts" -> 0,"provider_variables" -> 0)
  //val SampleValues: scala.collection.mutable.Map[String,Int] = scala.collection.mutable.Map("to"->0, "bcc"->0, "cc"->0, "recipients"->0)
  //val SampleValues: scala.collection.mutable.Map[String,Int] = scala.collection.mutable.Map("geolocation.coordinates"->0,"geolocation"->0)

  val StringClass = classOf[String]
  val DoubleClass = classOf[java.lang.Double]
  val BooleanClass = classOf[java.lang.Boolean]
  val ArrayClass = classOf[java.util.ArrayList[_]]
  val ObjectClass = classOf[com.google.gson.internal.LinkedTreeMap[_,_]]
  val Gson = new Gson()
  val MapType = new java.util.HashMap[String,Object]().getClass

  // AttributeName -> (Type, Count)
  val GlobalTypeMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]] = scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]]()


  def map(row: String): Boolean = {
    try {
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
      val attributeName = prefix + attribute._1
      val attributeValue = attribute._2
      if(Sample){
        SampleValues.get(attributeName) match {
          case Some(c) =>
            if (c < SampleLimit){
              println(attributeName + " : " + attributeValue)
              SampleValues.update(attributeName,c+1)
            }
          case None => // do nothing
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
          returnType += "String,"

        case(DoubleClass) =>
          update2(attributeName,"Double")
          returnType += "Double,"

        case(BooleanClass) =>
          update2(attributeName, "Boolean")
          returnType += "Boolean,"

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
          returnType += "Array,"

        case(ObjectClass) =>
          val t = mapInsertType(attributeName + ".", Gson.fromJson(Gson.toJson(attributeValue), MapType))
          update2(attributeName, s"{$t}")
          returnType += "Object,"

        case(null) =>
          if(IncludeNulls) {
            update2(attributeName, "Null")
            returnType += "Null,"
          }
        case _ =>
          update2(attributeName,"UnknownType")
          returnType += "UnknownType,"
      }
    })
    if(returnType.equals(""))
      return ""
    else
      return returnType.substring(0,returnType.size-1) // remove the last comma for split
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

  val ArrayToObject: ListBuffer[String] = ListBuffer[String]()
  val ObjectToArray: ListBuffer[String] = ListBuffer[String]()
  val ArrayAndObject: ListBuffer[String] = ListBuffer[String]() // contains both array and object types
  val ConfirmedAsArray: ListBuffer[String] = ListBuffer[String]() // confirmed more likely to be an array
  val TupleType: ListBuffer[String] = ListBuffer[String]() // could be better represented as a tuple

  // with this then generate a schema and loop back over the dataset with the schema and create the feature vectors
  def reduce() = {
    // iterate through the map and find all { for objects and [ for all
    GlobalTypeMap.foreach((x) => {
      val xAttributeName = x._1
      val xTypeMap = x._2
      var isArray = false
      var isObject = false
      var hypothesisType: String = null
      var hypothesisSize: Int = -1
      var variableSize = false
      var multipleType = false
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
              val typeList = yTypeName.substring(1, yTypeName.size - 1).split(',')
              if (hypothesisSize == -1)
                hypothesisSize = typeList.size
              else if (hypothesisSize != typeList.size)
                variableSize = true
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
              val typeList = yTypeName.substring(1, yTypeName.size - 1).split(',')
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
      //  println(xTypeMap)
      if(isArray && isObject) // has both array and object types
        ArrayAndObject += xAttributeName
      else if(isArray && !multipleType)
        ConfirmedAsArray += xAttributeName
      else if(isArray && multipleType && variableSize) // it's children should be kept
        ArrayToObject += xAttributeName
      else if(isObject && !multipleType && variableSize) // should be an array
        ObjectToArray += xAttributeName
      else if(multipleType && !variableSize) // could be better represented as a tuple potentionally
        TupleType += xAttributeName

      if(Sample){
        SampleValues.get(xAttributeName) match {
          case Some(c) =>
            println(xAttributeName + " : " + xTypeMap)
          case None => // do nothing
        }
      }
    })
    println("Confirmed Arrays: "+ConfirmedAsArray)
    println("Arrays to Objects: "+ArrayToObject)
    println("Objects to Arrays: "+ObjectToArray) // retweeted_status.entities, entities, quoted_status.entities, retweeted_status.quoted_status.entities
    println("Tuples: "+TupleType)
    println("Had both Arrays and Objects: "+ArrayAndObject)
    println("Done")
  }




  import scala.collection.JavaConverters._


  val combineArrays: Boolean = true // flatten arrays into one parent object
  val failSilently: Boolean = false // won't throw an error for malformed json if true
  val filterNulls: Boolean = false // check values for null, if null then don't count it, this might improve clustering but will take way longer

  var totalSchemaMap: scala.collection.mutable.HashMap[String,Int] = scala.collection.mutable.HashMap[String,Int]() // the total schema, used to determine the feature vector
  var jsonShapeFormat: scala.collection.mutable.Map[ListBuffer[Int],Int] = scala.collection.mutable.Map[ListBuffer[Int],Int]() // holds the feature vector based on the json shape as the key and the multiplicity as the value
  var schemaSize: Int = 0

  def xStep(s: String): Unit = {

      val jsonString: String = s
      var jsonLeafKeySet: Set[String] = null

      try {
        val jsonMap: java.util.Map[String,AnyRef] = JsonFlattener.flattenAsMap(jsonString) // create a flat map of the json object
        if(combineArrays)
          jsonLeafKeySet = combineArr(jsonMap.keySet())
        else
          jsonLeafKeySet = jsonMap.keySet()


        // update total schema based on new row
        jsonLeafKeySet.asScala.foreach((v: String) => {
          if(!totalSchemaMap.contains(v)) {
            totalSchemaMap.put(v, schemaSize)
            schemaSize += 1
          }
        })

        val featureVector: scala.collection.mutable.ArrayBuffer[Int] = scala.collection.mutable.ArrayBuffer.fill[Int](schemaSize+1)(0)

        jsonLeafKeySet.asScala.foreach((v) => {
          featureVector(totalSchemaMap.get(v).get) = 1
        })
        /*
                val featureVector: Array[Int] = scala.Array.fill[Int](totalSchema.size)(0) // initialize an empty feature, then fill with 1's
                jsonLeafKeySet.asScala.foreach((v: String) => {
                  totalSchema.index
                })
        */
        val norm = removeTailZeros(featureVector.toList) // this normalizes the feature vector for map insertion
        jsonShapeFormat.get(norm) match {
          case Some(i) => jsonShapeFormat += (norm -> (i+1))
          case None => jsonShapeFormat += (norm -> 1)
        }

      } catch{ // try catch for shredder
        case e: Exception => {
          if(!failSilently)
            println(s"Not of JSON format in Json_Explorer_Project, so null returned: $jsonString")
        } // is not a proper json format so return null since there's nothing we can do about this right now
      } // end try catch

  }



  def xFinal(): Unit = {

    try { // try to write output

      // read in output directory
      val reader = new BufferedReader(new FileReader(new File("clusterOptions.txt")))
      val dir: String = reader.readLine()

      val shapeVectorOutputPath: String = s"$dir\\fvoutput.txt"
      val shapeMultiplicityOutputPath: String = s"$dir\\multoutput.txt"
      val schemaOutputPath: String = s"$dir\\schema.txt"

      val shapeVectorWriter: BufferedWriter = new BufferedWriter(new FileWriter(shapeVectorOutputPath))
      val shapeMultWriter: BufferedWriter = new BufferedWriter(new FileWriter(shapeMultiplicityOutputPath))
      val schemaWriter: BufferedWriter = new BufferedWriter(new FileWriter(schemaOutputPath))

      jsonShapeFormat.foreach((v) => {
        val featureVector: ListBuffer[Int] = v._1
        val multiplicity: Int = v._2

        var vectorOutput: String = ""
        var oneCount: Int = 0   // count of how many 1's occur
        featureVector.zipWithIndex.foreach((i) => {
          val loc = i._2

          if(i._1 != 0){
            oneCount += 1
            vectorOutput += s" $loc:1" // prepare the output
          }
        })
        val output: String = oneCount + vectorOutput
        if(multiplicity > 10) {
          shapeVectorWriter.write(output + '\n')
          shapeMultWriter.write(multiplicity.toString + '\n')
        }
      })

      shapeVectorWriter.close()
      shapeMultWriter.close()

      var schema = ""
      totalSchemaMap.foreach((s) => schema = schema + s._1 + ",")
      schemaWriter.write(schema.substring(0,schema.size-1))
      schemaWriter.close()

    } catch {
      case e: Exception => throw e
    }
    cleanUp() // clean up containers
  }

  def cleanUp(): Unit = { // apparently you need to clean up agg functions in 2017 even though it's a class :(
    totalSchemaMap.clear()
    jsonShapeFormat.clear()
    schemaSize = 0
  }

  def combineArr(paths: Set[String]): Set[String] = {
    val ret: Set[String] = new util.HashSet[String]()
    val pathIterator = paths.iterator()
    while(pathIterator.hasNext){
      val path: String = pathIterator.next()
      if(path.contains("[")) { // is an array
        // this unreadible line of code will first remove everything between square brackets, then it will replace all multiple periods with a single period for sanity
        ret.add(path.replaceAll("\\[.*\\]", "").replaceAll("\\.{2,}", "\\."))
      }
      else
        ret.add(path) // not an array
    }
    ret
  }

  def removeTailZeros(l: List[Int]): ListBuffer[Int] = {
    // removes tailing zeros from a list to normalize it
    var ret: ListBuffer[Int] = ListBuffer[Int]()
    val temp: ListBuffer[Int] = ListBuffer[Int]()
    l.foreach((v)=> {
      if(v == 0)
        temp += 0
      else {
        temp += 1
        ret = ret ++ temp
        temp.clear()
      }
    })
    ret
  }




}
