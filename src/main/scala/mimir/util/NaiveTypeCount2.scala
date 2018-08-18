package mimir.util

import java.io._
import java.util

import com.google.gson.{Gson, GsonBuilder}
import scalafx.Includes._
import scalafx.scene.control._
import scalafx.application
import scalafx.application.JFXApp
import scalafx.event.ActionEvent
import scalafx.scene.Scene
import scalafx.scene.layout.{FlowPane, Priority}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}



class NaiveTypeCount2 {

  val StringClass = classOf[String]
  val DoubleClass = classOf[java.lang.Double]
  val BooleanClass = classOf[java.lang.Boolean]
  val ArrayClass = classOf[java.util.ArrayList[_]]
  val ObjectClass = classOf[com.google.gson.internal.LinkedTreeMap[_,_]]
  val Gson = new Gson()
  val MapType = new java.util.HashMap[String,Object]().getClass

  /**
    * Handles building and running the system from building all the maps to computing entropy and visualizations
    *
    * @param datasetName Dataset name, used for cacheing and uncacheing as well as in visualizations
    * @param inputFile the inputFile that contains the json records
    * @param RowLimit read only this many rows, 0 for the entire file
    * @param Naive
    * @param Verbose
    * @param Sample
    * @param SampleLimit
    * @param Stash
    * @param Unstash
    * @param Visualize
    * @param hasSchema
    */
  def run(datasetName: String, inputFile: File, RowLimit: Int = 0, Naive: Boolean = false, Verbose: Boolean = false, Sample: Boolean = false, SampleLimit: Int = 10, Stash: Boolean = false, Unstash: Boolean = true, Visualize: Boolean = true, hasSchema: Boolean = false): Unit = {
    var TypeMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]] = scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]]()
    var SampleMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.ListBuffer[String]] = scala.collection.mutable.HashMap[String,scala.collection.mutable.ListBuffer[String]]() // a list of samples for each attribute
    var KeyEntropyList: ListBuffer[(Double,String)] = ListBuffer[(Double,String)]()
    if(Unstash) {
      val x = unstash(datasetName)
      TypeMap = x._1
      SampleMap = x._2
      KeyEntropyList = x._3
    } else{
      val x = buildMaps(inputFile, RowLimit, Naive, Sample, SampleLimit, Verbose) // builds typeMap, sampleMap
      TypeMap = x._1
      SampleMap = x._2
      KeyEntropyList = calculateEntropy(TypeMap)
    }
    if(Stash)
      stash(datasetName,TypeMap,SampleMap,KeyEntropyList,Sample)
    val (arrayList,tupleList, dictList) = detectComplexTypes(KeyEntropyList,1.0, 1.0)
    println("Done")
  }


    /** Builds a TypeMap and SampleMap for the input file
      * - TypeMap is a map of the attribute names pointing to a map of its unique schemas and respective counts
      * - SampleMap is a list of data samples for each attribute, used for inspection and visualizations to gain insight
      *
      * @param inputFile the inputFile that contains the json records
      * @param rowLimit read only this many rows, 0 for the entire file
      * @param naive import style, if true then readline if false then take the substring that is the fist { and last } for each row
      * @param sample if true then create and fill SampleMap
      * @param sampleLimit the maximum number of samples for each attribute in SampleMap
      * @param verbose print various debugging information
      * @return the result of squaring d
    */
  private def buildMaps(inputFile: File, rowLimit: Int = 0, naive: Boolean, sample: Boolean, sampleLimit: Int, verbose: Boolean): (scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]], scala.collection.mutable.HashMap[String,scala.collection.mutable.ListBuffer[String]]) = {
    val loadJson: LoadJson2ElectricBoogaloo = new LoadJson2ElectricBoogaloo(inputFile, naive=naive)
    val TypeMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]] = scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]]()
    val SampleMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.ListBuffer[String]] = scala.collection.mutable.HashMap[String,scala.collection.mutable.ListBuffer[String]]() // a list of samples for each attribute
    val rejectedRows: ListBuffer[String] = scala.collection.mutable.ListBuffer[String]()

    var line: String = loadJson.getNext()
    var rowCount: Int = 0
    var nonJsonRowCount: Int = 0

    while((line != null) && ((rowCount < rowLimit) || (rowLimit < 1))){
      val added = add(line, TypeMap, SampleMap, sample, sampleLimit, verbose)
      if(added)
        rowCount += 1
      else if(!added && !line.equals("")){
        nonJsonRowCount += 1
        rejectedRows += line
      }
      if((rowCount % 100000 == 0) && added && verbose)
        System.err.println(s"$rowCount rows added, $nonJsonRowCount rows rejected so far")
      line = loadJson.getNext()
    }
    loadJson.close()
    if(verbose)
      System.err.println(s"$rowCount rows added, $nonJsonRowCount rows rejected")

    return Tuple2(TypeMap,SampleMap)
  }

  private def combineTypeMap(parentMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]], childMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]]): Unit = {
    childMap.foreach((x) => {
      x._2.foreach(y => {
        updateMap(x._1,y._1,parentMap)
      })
    })
  }

  /** adds the rows type
    *
    * @return returns true if the json row was added, false if a problem occurred
  */
  private def add(row: String, typeMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]], sampleMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.ListBuffer[String]], sample: Boolean, sampleLimit: Int, verbose: Boolean): Boolean = {
    try {
      if(row.equals(""))
        return false
      val m: java.util.HashMap[String, Object] = Gson.fromJson(row, MapType)
      val rowTypeMap = scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]]()
      val rowSampleMap = scala.collection.mutable.HashMap[String,String]()
      mapGetType("", m, rowTypeMap, rowSampleMap)
      combineTypeMap(typeMap,rowTypeMap)
    } catch {
      case e: com.google.gson.JsonSyntaxException =>
        return false
    }
    return true
  }

  /**
    * Inserts every attributes type in TypeMap and its attribute sample into SampleMap
    * Will return simple type names but will update objects and arrays in the map to be one layer deeper such as {a:String, b:Int}
    *
    * @param prefix prefix string to be prepended to the
    * @param jsonMap input map that was created from Json
    * @param TypeMap passes map for recurrsion as a structure
    * @param SampleMap passes map for recurrsion as a structure
    * @return returns a string that is the type for objects
    */
  private def mapGetType(prefix: String, jsonMap: java.util.HashMap[String,Object], TypeMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]], SampleMap: scala.collection.mutable.HashMap[String,String]): String = {
    var returnType: String = ""
    jsonMap.asScala.foreach(attribute => {
      val attributeName = prefix + attribute._1.replace(",",";").replace(":",";")
      val attributeValue = attribute._2
      //SampleMap.update(attributeName,attributeValue.toString)

      var attributeClass: Class[_ <: Object] = null
      try {
        attributeClass = attribute._2.getClass
      } catch {
        case e: java.lang.NullPointerException => // do nothing
      }
      attributeClass match {
        case(StringClass) =>
          updateMap(attributeName, "String", TypeMap)
          returnType += s"$attributeName:String,"

        case(DoubleClass) =>
          updateMap(attributeName,"Double", TypeMap)
          returnType += s"$attributeName:Double,"

        case(BooleanClass) =>
          updateMap(attributeName, "Boolean", TypeMap)
          returnType += s"$attributeName:Boolean,"

        case(ArrayClass) =>
          val attributeList = attributeValue.asInstanceOf[java.util.ArrayList[java.lang.Object]].asScala
          var arrayType = ""
          attributeList.zipWithIndex.foreach(x => {
            val arrayMap: java.util.HashMap[String, java.lang.Object] = new java.util.HashMap[String, java.lang.Object]()
            arrayMap.put(s"[${x._2}]", x._1)
            arrayType += mapGetType(attributeName, arrayMap, TypeMap, SampleMap) + ","
          })
          if(!arrayType.equals(""))
            arrayType = arrayType.substring(0, arrayType.size - 1)
          updateMap(attributeName, s"[$arrayType]", TypeMap)
          returnType += s"$attributeName:Array,"

        case(ObjectClass) =>
          val t = mapGetType(attributeName + ".", Gson.fromJson(Gson.toJson(attributeValue), MapType), TypeMap, SampleMap)
          updateMap(attributeName, s"{${sortObject(t)}}", TypeMap) // this is done because order shouldn't matter
          returnType += s"$attributeName:Object,"

        case(null) =>
          updateMap(attributeName, "Null", TypeMap)
            returnType += s"$attributeName:Null,"
        case _ =>
          updateMap(attributeName,"UnknownType", TypeMap)
          returnType += s"$attributeName:UnknownType,"
      }
    })
    if(returnType.equals(""))
      return ""
    else
      return returnType.substring(0,returnType.size-1) // remove the last comma for split
  }

  /**
    * @param typeString a comma separated list that's a string, this will sort on attribute name for easier future comparison
    * @return returns a string that is the input string but sorted
    */
  private def sortObject(typeString:String): String= {
    val typeArray = typeString.split(",")
    val sorted = typeArray.sortBy(f => f.split(":")(0))
    return sorted.mkString(",")
  }

  /**
    * updates map so that the outer keys inner keys count is incremented by 1 if it occurs or is added
    *
    * @param outerKey attribute name
    * @param innerKey type name
    * @param map increments the inner key count or initializes it to 1 for the outer key
    */
  def updateMap(outerKey: String, innerKey: String, map: scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]]): Unit = {
    map.get(outerKey) match {
      case Some(typeMap) =>
        typeMap.get(innerKey) match {
          case Some(count) =>
            typeMap.update(innerKey, count + 1)
          case None =>
            typeMap.update(innerKey,1)
        }
        map.update(outerKey,typeMap)
      case None => map.update(outerKey, scala.collection.mutable.HashMap(innerKey -> 1))
    }
  }

  // takes the global map that is filled with AttributeNames -> Map(Types,Count)
  // reduce wants to find if arrays and objects that have multiple types
  // - easy, just check the map size is > 1 if it contains [ or {
  // also wants to find if arrays and objects are variable size
  // - need to find all attributes of the array or object at the same path level and determine if all their counts are equal

  // with this then generate a schema and loop back over the dataset with the schema and create the feature vectors
  /**
    * calculates the key-space and type entropy for each attribute
    *
    * @param TypeMap
    * @return returns a list of tuples, (score, json) the json value is a represnetation of the attribute and it's scores and values
    */
  def calculateEntropy(TypeMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]]): scala.collection.mutable.ListBuffer[(Double,String)] = {

    val KeyEntropyList = new ListBuffer[(Double,String)]()
    // iterate through the map and find all { for objects and [ for all
    TypeMap.foreach((everyAttribute) => {
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
        val typeData: (Double, Int) = keySpace.foldLeft((0.0,0)){(acc,y) =>
          val typeName = y._1
          val typeCount = y._2
          var inc = 0
          var localTypeEntropy: Double = 0.0
          if(!typeName.equals("[]") && !typeName.equals("{}") && !typeName.equals("Null")){
            localTypeEntropy = typeEntropy(typeName.substring(1,typeName.size-1))
            inc += 1
          } // else empty array/object so 0.0 for entropy
          (acc._1 + localTypeEntropy,acc._2 + inc)
        }
        var tEntropy: Double = 0.0
          if (typeData._2 != 0)
            tEntropy = (typeData._1 / typeData._2.toDouble)
        val score: Double = ksEntropy/(1.0+tEntropy)
        val jsonOutput: String = s"""{"title":"${attributeName}","keySpaceEntropy":$ksEntropy,"typeEntropy":$tEntropy,"entropyScore":$score,"type":"$objType","x":[${xValue.mkString(",")}],"y":[${yValue.mkString(",")}]}"""
        KeyEntropyList += Tuple2(score,jsonOutput)

      } else {
        // is a primitive Type i.e String, numeric, boolean
      }
    })

    return KeyEntropyList.sortBy(_._1)(Ordering[Double].reverse)//.foreach(t=>KeyEntropyFile.write(t._2+'\n'))
  }


  /**
    * computes type entropy for an object/ array
    *
    * @param unpackedTypeList
    * @return
    */
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

  /**
    * computes the keyspace entropy for an object/ array
    *
    * @param m
    * @return
    */
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

  // would return "a":[{b,c},{b,d}] => "a":{b,c}
  // makes sense for twitter, but does it generalize?
  // maybe something where all objects in arrays
  // if you pull out all objects in an array there may be a natural join that is the array index, can add that field
  private def Abadi(): Unit = {

  }


  private def detectComplexTypes(KeyEntropyList: ListBuffer[(Double,String)], arrayThreshold: Double, objectThreshold: Double):(ListBuffer[String],ListBuffer[String],ListBuffer[String]) = {
    val arrayAttributes: ListBuffer[String] = ListBuffer[String]()
    val tupleAttributes: ListBuffer[String] = ListBuffer[String]()
    val dictAttributes: ListBuffer[String] = ListBuffer[String]()
    val arraysWithObjects: ListBuffer[String] = ListBuffer[String]()

    KeyEntropyList.foreach(x => {
      val score: Double = x._1
      val json: String = x._2
      val jsonMap = Gson.fromJson(json,MapType)
      val attributeName: String = jsonMap.get("title").toString
      val structureType: String = jsonMap.get("type").toString
      val keySpaceEntropy: Double = jsonMap.get("keySpaceEntropy").toString.toDouble
      val typeEntropy: Double = jsonMap.get("typeEntropy").toString.toDouble


      if(structureType.equals("Array")){
        if(score < 1.0)
          println(attributeName)
        if(typeEntropy == 0.0)
          arrayAttributes += attributeName
        if(typeEntropy > 0.0) {
          System.err.println(s"$attributeName is an array with multiple types")
          System.err.println(jsonMap.get("x"))
        }
        jsonMap.get("x").asInstanceOf[java.util.ArrayList[Object]].asScala.foreach( z => {
          z.toString.substring(1,z.toString.length-1).split(",").foreach( y => {
            if(!y.equals("")) {
              val childName = y.toString.split(":")(0)
              val childType = y.toString.split(":")(1)
              if (childType.equals("Object"))
                arraysWithObjects += childName
            }
          })
        })

      } else if(structureType.equals("Object")){
          //if(score > 1.0)
            //println(attributeName)
          if(keySpaceEntropy < objectThreshold) // dictionary
            tupleAttributes += attributeName
          else // tuple
            dictAttributes += attributeName

      } else { // unknown
        System.err.println("Unknown type found in detectComplexTypes")
      }

    })

    return (arrayAttributes, tupleAttributes, dictAttributes)
  }

  def generateSchema(TypeMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]], arrayAttributes: ListBuffer[String], tupleAttributes: ListBuffer[String], verbose: Boolean): ListBuffer[String] = {
    val schema: ListBuffer[String] = ListBuffer[String]()
    var reducedAsArray = 0
    var reducedAsTuple = 0
    TypeMap.map(x =>{
      val excludeAsArray: Boolean = arrayAttributes.foldLeft(false){(acc,y) => {
        if(!acc)
          isAChildOrEqual(y,x._1)
        else
          acc
      }}
      val excludeAsTuple: Boolean = tupleAttributes.foldLeft(false){(acc,y) => {
        if(!acc)
          isAChildOrEqual(y,x._1)
        else
          acc
      }}
      if(excludeAsArray)
        reducedAsArray += 1
      else if(excludeAsTuple)
        reducedAsTuple += 1
      else
        schema += x._1
    })
    if(verbose) {
      println(s"$reducedAsArray attributes were reduced as an array")
      println(s"$reducedAsTuple attributes were reduced as tuples")
    }
    return schema
  }


  def generateFeatureVectors(inputFile: File, naive: Boolean, rowLimit: Int, schema: mutable.HashSet[(String,Int)], fvMap: scala.collection.mutable.HashMap[String,Int]) = {
    val loadJson: LoadJson2ElectricBoogaloo = new LoadJson2ElectricBoogaloo(inputFile, naive=naive)
    var line: String = loadJson.getNext()
    var rowCount: Int = 0
    var nonJsonRowCount: Int = 0

    while((line != null) && ((rowCount < rowLimit) || (rowLimit < 1))){
      try {
        val m: java.util.HashMap[String, Object] = Gson.fromJson(line, MapType)
        val fv = getFeatureVector("",m,ArrayBuffer.fill(schema.size)(0.0), schema).mkString(",")
        fvMap.get(fv) match {
          case Some(c) => fvMap.update(fv,c+1)
          case None => fvMap.update(fv,1)
        }
        if(rowCount % 100000 == 0)
          println(s"Row Count: $rowCount")
        rowCount += 1
      } catch {
        case e: com.google.gson.JsonSyntaxException =>
          nonJsonRowCount += 1
      }
      line = loadJson.getNext()
    }
    loadJson.close()
    val fvWriter = new BufferedWriter(new FileWriter(s"cache/fvoutput.txt"))
    val multWriter = new BufferedWriter(new FileWriter(s"cache/multoutput.txt"))
    val schemaWriter = new BufferedWriter(new FileWriter(s"cache/schema.txt"))
    fvMap.foreach(x => {
      val r = x._1.split(",").zipWithIndex.map(y => s"${y._2}:${y._1}")
      fvWriter.write(s"${r.size} ${r.mkString(" ")}\n")
      multWriter.write(s"${x._2}\n")
    })
    schemaWriter.write(schema.toList.sortBy(_._2).map(_._1).mkString(","))
    fvWriter.close()
    multWriter.close()
    schemaWriter.close()
  }

  def getFeatureVector(prefix: String, m: java.util.HashMap[String,Object], fv:ArrayBuffer[Double], schema: mutable.HashSet[(String,Int)]): ArrayBuffer[Double] = {
    m.asScala.foreach(attribute => {
      val attributeName = prefix + attribute._1.replace(",",";").replace(":",";")
      val attributeValue = attribute._2
      var attributeClass: Class[_ <: Object] = null
      try {
        attributeClass = attribute._2.getClass
      } catch {
        case e: java.lang.NullPointerException => // do nothing
      }
      attributeClass match {
        case(StringClass) | (DoubleClass) | (BooleanClass) =>
          schema.find(_._1.equals(attributeName)) match {
            case Some(x) =>
              fv(x._2) = 1.0
            case None =>
          }

        case(ArrayClass) =>
          // need to inspect children for objects
          schema.find(_._1.equals(attributeName)) match {
            case Some(x) =>
              fv(x._2) = 1.0
            case None =>
          }

        case(ObjectClass) =>
          schema.find(_._1.equals(attributeName)) match {
            case Some(x) =>
              fv(x._2) = 1.0
              getFeatureVector(attributeName+".",Gson.fromJson(Gson.toJson(attributeValue), MapType),fv, schema)
            case None =>
          }

        case(null) =>
          schema.find(_._1.equals(attributeName)) match {
            case Some(x) =>
              fv(x._2) = 1.0
            case None =>
          }
        case _ =>
          throw new Exception("Unknown Type in FV Creation.")
      }
    })
    return fv
  }

  def isAChildOrEqual(parentAttribute: String, childAttribute: String): Boolean = {
    val parentList: List[String] = parentAttribute.split("\\.").toList
    val childList: List[String] = childAttribute.split("\\.").toList
    if(parentList.size == 0 || childList.size == 0 || parentAttribute.equals("") || childAttribute.equals(""))
      throw new Exception(s"Something went wrong with either $parentAttribute or $childAttribute in function isAChild()")
    if(childList.size < parentList.size)
      return false
    else if(childList.size == parentList.size){
      val left = childAttribute.lastIndexOf('[')
      val right = childAttribute.lastIndexOf(']')
      if(left != -1 && right != -1)
        return parentAttribute.equals(childAttribute.substring(0,left))
      else
        return false
    } else
      parentList.zipWithIndex.foreach(x => {
        if(!x._1.equals(childList(x._2)))
          return false
      })
    return true
  }

  // retrieves the data that was stashed
  // loads KeyEntropyList, GlobalTypeMap, and SampleMap
  def unstash(datasetName: String): (scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]], scala.collection.mutable.HashMap[String,scala.collection.mutable.ListBuffer[String]], ListBuffer[(Double,String)]) = {
    val sampleInput: BufferedReader = new BufferedReader(new FileReader(s"cache/${datasetName}Sample.json"))
    val typeInput: BufferedReader = new BufferedReader(new FileReader(s"cache/${datasetName}Types.json"))
    val entropyInput: BufferedReader = new BufferedReader(new FileReader(s"cache/${datasetName}Entropy.json"))

    val TypeMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]] = scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]]()
    val SampleMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.ListBuffer[String]] = scala.collection.mutable.HashMap[String,scala.collection.mutable.ListBuffer[String]]() // a list of samples for each attribute
    val KeyEntropyList: ListBuffer[(Double,String)] = ListBuffer[(Double,String)]()

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
      TypeMap.update(x._1, localMap)
    })
    typeInput.close()

    var entropyLine: String = entropyInput.readLine()
    while(entropyLine != null){
      val split: Int = entropyLine.indexOf(',')
      KeyEntropyList += Tuple2(entropyLine.substring(0,split).toDouble,entropyLine.substring(split+1))
      entropyLine = entropyInput.readLine()
    }
    entropyInput.close()
    return Tuple3(TypeMap, SampleMap, KeyEntropyList)
  }


  /**
    * Stores these maps for unstash to reload. Stored in the cache directory
    *
    * @param datasetName used for the file name/ path
    * @param TypeMap
    * @param SampleMap
    * @param KeyEntropyList
    * @param sample
    */
  def stash(datasetName: String, TypeMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]], SampleMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.ListBuffer[String]], KeyEntropyList: ListBuffer[(Double,String)], sample: Boolean) = {
    val sampleWriter = new BufferedWriter(new FileWriter(s"cache/${datasetName}Sample.json"))
    val typeWriter = new BufferedWriter(new FileWriter(s"cache/${datasetName}Types.json"))
    val entropyWriter = new BufferedWriter(new FileWriter(s"cache/${datasetName}Entropy.json"))

    if(sample){
      SampleMap.foreach(x => sampleWriter.write(s"""{"title":"${x._1}","payload":[${x._2.map(Gson.toJson(_)).mkString(",")}]}\n"""))
      sampleWriter.flush()
      sampleWriter.close()
    }

    typeWriter.write("{"+TypeMap.map(x => {
      val localJsonTypeMap = "{"+x._2.map(y => s""""${y._1}":${y._2}""").mkString(",")+"}"
      s""""${x._1}":${localJsonTypeMap}"""
    }).mkString(",")+"}")
    typeWriter.flush()
    typeWriter.close()

    KeyEntropyList.foreach(x => entropyWriter.write(s"""${x._1},${x._2}\n"""))
    entropyWriter.flush()
    entropyWriter.close()
  }

}