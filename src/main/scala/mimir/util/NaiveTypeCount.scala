package mimir.util

import com.github.wnameless.json.flattener.JsonFlattener

import scala.collection.mutable.ListBuffer

class NaiveTypeCount {

  // ColumnName -> Map[Type,Count]
  private val mainMap: scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]] = new scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]]()
  private val countNulls = false

  // shreds json row and adds it to the mainMap
  // take in row, pull out schema and type, only checking arrays down one layer
  def add(row: String) = {
    // first update all the types, in the keyspace, then update all the objects
    val jsonMap: java.util.Map[String,AnyRef] = JsonFlattener.flattenAsMap(row)
    val keySet = jsonMap.keySet().toArray()
    // help me find stuff in debugging
    keySet.foreach(a => {
      //if(a.toString.contains("categories") || a.toString.contains("friends") || a.toString.contains("elite"))
      //if(a.toString.contains("friends") || a.toString.contains("elite"))
      if(a.toString.contains("scopes.place_ids"))
        println(a.toString + " : " + jsonMap.get(a.toString).toString)
    })

    var i: Int = 0
    val len = keySet.length
    for(i <- 0 until len){
      val attribute: String = keySet(i).toString
      var attributeType: Class[_ <: AnyRef] = null
      try {
        attributeType = jsonMap.get(attribute).getClass
      }
      catch {
        case e : Exception  =>
      }

      if(attributeType == classOf[java.lang.String]){
        update(attribute,"String")
      } else if (attributeType == classOf[java.lang.Boolean]){
        update(attribute,"Boolean")
      }
      else if (attributeType == classOf[java.math.BigDecimal]){
        update(attribute,"Numeric")
      } else if (attributeType == null && countNulls){
        update(attribute,"Null")
      } /*else {
        println("UNKNOWN TYPE")
      }*/
    }
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


  private val arrayCondense = new ListBuffer[String]()
  private val otherThing = new ListBuffer[String]() // fixed size and multiple types
  private val badApples = new ListBuffer[String]()
  // Path -> Map(Type+Size,Count)
  private val arrayCandidates = new scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,Int]]()
  // after all added call report to get the output
  def report() = {
    val mainMapKeys = mainMap.keySet
    mainMap.foreach(t => {
      val columnName: String = t._1
      val typeMap: scala.collection.mutable.HashMap[String,Int] = t._2
      if(columnName.contains('[')){
        val path: String = columnName.substring(0,columnName.indexOf('['))
        val index: String = columnName.substring(columnName.indexOf('[')+1,columnName.indexOf(']'))
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
        arrayCondense += colName +":"+arraySize.toString
      else if(multipleFound && fixedSizeBool && !badApples.contains(colName) && numberOfTypes > 1)
        otherThing += colName +":"+arraySize.toString
    })
    println(arrayCondense)
    println(otherThing)
    println(badApples)
  }

}
