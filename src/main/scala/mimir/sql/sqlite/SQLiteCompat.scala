package mimir.sql.sqlite

import java.util
import java.util.Set

import com.github.wnameless.json.flattener.JsonFlattener
import mimir.algebra._
import mimir.provenance._
import mimir.util.{JDBCUtils, JsonPlay, JsonUtils}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.geotools.referencing.datum.DefaultEllipsoid
import org.joda.time.DateTime
import com.typesafe.scalalogging.slf4j.LazyLogging
import play.api.libs.json._
import mimir.util.JsonPlay._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json._
import scala.xml.Utility._


object SQLiteCompat extends LazyLogging{

  val INTEGER = 1
  val FLOAT   = 2
  val TEXT    = 3
  val BLOB    = 4
  val NULL    = 5

  def registerFunctions(conn:java.sql.Connection):Unit = {
    org.sqlite.Function.create(conn,"MIMIRCAST", MimirCast)
    org.sqlite.Function.create(conn,"MIMIR_MAKE_ROWID", MimirMakeRowId)
    org.sqlite.Function.create(conn,"OTHERTEST", OtherTest)
    org.sqlite.Function.create(conn,"AGGTEST", AggTest)
    org.sqlite.Function.create(conn, "SQRT", Sqrt)
    org.sqlite.Function.create(conn, "DST", Distance)
    org.sqlite.Function.create(conn, "SPEED", Speed)
    org.sqlite.Function.create(conn, "MINUS", Minus)
    org.sqlite.Function.create(conn, "GROUP_AND", GroupAnd)
    org.sqlite.Function.create(conn, "GROUP_OR", GroupOr)
    org.sqlite.Function.create(conn, "GROUP_BITWISE_AND", GroupBitwiseAnd)
    org.sqlite.Function.create(conn, "GROUP_BITWISE_OR", GroupBitwiseOr)
    org.sqlite.Function.create(conn, "FIRST", First)
    org.sqlite.Function.create(conn, "FIRST_INT", FirstInt)
    org.sqlite.Function.create(conn, "FIRST_FLOAT", FirstFloat)
    org.sqlite.Function.create(conn, "POSSION", Possion)
    org.sqlite.Function.create(conn, "GAMMA", Gamma)
    org.sqlite.Function.create(conn, "STDDEV", StdDev)
    org.sqlite.Function.create(conn, "MAX", Max)
    org.sqlite.Function.create(conn, "JSON_EXPLORER_MERGE", JsonExplorerMerge)
    org.sqlite.Function.create(conn, "JSON_EXPLORER_PROJECT", JsonExplorerProject)
    org.sqlite.Function.create(conn, "JSON_CLUSTER_PROJECT", JsonClusterProject)
    org.sqlite.Function.create(conn, "JSON_CLUSTER_AGG", new JsonClusterAGG())
  }
  
  def getTableSchema(conn:java.sql.Connection, table: String): Option[Seq[(String, Type)]] =
  {
    // Hardcoded table schemas:
    table.toUpperCase match {
      case "SQLITE_MASTER" => 
        return Some(Seq(
            ("NAME", TString()),
            ("TYPE", TString())
          ))
      case _ => ()
    }

    val stmt = conn.createStatement()
    val ret = stmt.executeQuery(s"PRAGMA table_info('$table')")
    stmt.closeOnCompletion()
    val result = JDBCUtils.extractAllRows(ret).map( (x) => { 
      val name = x(1).asString.toUpperCase.trim
      val rawType = x(2).asString.trim
      val baseType = rawType.split("\\(")(0).trim
      val inferredType = try {
        Type.fromString(baseType)
      } catch {
        case e:RAException => 
          logger.warn(s"While getting schema for table '$table': ${e.getMessage}")
          TAny()          
      }
      
      // println(s"$name -> $rawType -> $baseType -> $inferredType"); 

      (name, inferredType)
    })

    if(result.hasNext){ Some(result.toList) } else { None }
  }
}

object Possion extends org.sqlite.Function with LazyLogging {
  private var rng: scala.util.Random = new scala.util.Random(
    java.util.Calendar.getInstance.getTimeInMillis + Thread.currentThread().getId)
  def poisson_helper(mean:Double):Int = {
    val L = math.exp(-mean)
    var k = 0
    var p = 1.0
    do {
        p = p * rng.nextDouble()
        k+=1
    } while (p > L)
    k - 1
  }
  override def xFunc(): Unit = {
    if (args != 1) { throw new java.sql.SQLDataException("NOT THE RIGHT NUMBER OF ARGS FOR POSSION, EXPECTED 1") }
    val m = value_double(0) 
    result(poisson_helper(m))
  }
  
  
 }


object Gamma extends org.sqlite.Function with LazyLogging {
  private var rng: scala.util.Random = new scala.util.Random(
    java.util.Calendar.getInstance.getTimeInMillis + Thread.currentThread().getId)

  def sampleGamma(k: Double, theta: Double): Double = {
    var accept: Boolean = false
    if (k < 1) {
// Weibull algorithm
      val c: Double = (1 / k)
      val d: Double = ((1 - k) * Math.pow(k, (k / (1 - k))))
      var u: Double = 0.0
      var v: Double = 0.0
      var z: Double = 0.0
      var e: Double = 0.0
      var x: Double = 0.0
      do {
        u = rng.nextDouble()
        v = rng.nextDouble()
        z = -Math.log(u)
        e = -Math.log(v)
        x = Math.pow(z, c)
        if ((z + e) >= (d + x)) {
          accept = true
        }
      } while (!accept);
      (x * theta)
    } else {
// Cheng's algorithm
      val b: Double = (k - Math.log(4))
      val c: Double = (k + Math.sqrt(2 * k - 1))
      val lam: Double = Math.sqrt(2 * k - 1)
      val cheng: Double = (1 + Math.log(4.5))
      var u: Double = 0.0
      var v: Double = 0.0
      var x: Double = 0.0
      var y: Double = 0.0
      var z: Double = 0.0
      var r: Double = 0.0
      do {
        u = rng.nextDouble()
        v = rng.nextDouble()
        y = ((1 / lam) * Math.log(v / (1 - v)))
        x = (k * Math.exp(y))
        z = (u * v * v)
        r = (b + (c * y) - x)
        if ((r >= ((4.5 * z) - cheng)) || (r >= Math.log(z))) {
          accept = true
        }
      } while (!accept);
      (x * theta)
    }
  }

  override def xFunc(): Unit = {
    if (args != 2) { throw new java.sql.SQLDataException("NOT THE RIGHT NUMBER OF ARGS FOR GAMMA, EXPECTED 2") }
    val k = value_double(0) 
    val theta = value_double(1)
     result(sampleGamma(k, theta))
  }
  
   
  }

object Minus extends org.sqlite.Function with LazyLogging {
  override def xFunc(): Unit = {
    if (args != 2) { throw new java.sql.SQLDataException("NOT THE RIGHT NUMBER OF ARGS FOR MINUS, EXPECTED 2") }
    value_double(0) - value_double(1)
  }
  }

object Distance extends org.sqlite.Function with LazyLogging {
  override def xFunc(): Unit = {
    if (args != 4) { throw new java.sql.SQLDataException("NOT THE RIGHT NUMBER OF ARGS FOR DISTANCE, EXPECTED 4 -- LAT1, LON1, LAT2, LON2") }
    val lon1: Double = value_double(1)
    val lat1: Double = value_double(0)
    val lon2: Double = value_double(3)
    val lat2: Double = value_double(2)

    result(DefaultEllipsoid.WGS84.orthodromicDistance(lon1, lat1, lon2, lat2))
  }
}

object Speed extends org.sqlite.Function with LazyLogging {
  override def xFunc(): Unit = {
    if (args != 3) { throw new java.sql.SQLDataException("NOT THE RIGHT NUMBER OF ARGS FOR SPEED, EXPECTED 3 -- DISTANCE (METERS), STARING DATE, ENDING DATE") }
    val distance: Double = value_double(0)

    val startingDateText: String = value_text(1)
    val endingDateText: String = value_text(2)

    val startingDate: DateTime = DateTime.parse(startingDateText)
    var endingDate: DateTime = new DateTime
    if(endingDateText != null) {
      endingDate = DateTime.parse(endingDateText)
    }

    val numberOfHours: Long = Math.abs(endingDate.getMillis - startingDate.getMillis) / 1000 / 60 / 60

    result(distance / 1000 / numberOfHours) // kmph
  }
}

object Sqrt extends org.sqlite.Function with LazyLogging {
  override def xFunc(): Unit = {
    if (args != 1) { throw new java.sql.SQLDataException("NOT THE RIGHT NUMBER OF ARGS FOR SQRT, EXPECTED 1") }
    Math.sqrt(value_double(0))
  }
}

object MimirMakeRowId extends org.sqlite.Function {

  @Override
  def xFunc(): Unit = { 
    result(
      Provenance.joinRowIds(
        (0 until args) map { i => RowIdPrimitive(value_text(i)) }
      ).asString
    )
  }
}

object MimirCast extends org.sqlite.Function with LazyLogging {


    @Override
    def xFunc(): Unit = { // 1 is int, double is 2, 3 is string, 5 is null
      if (args != 2) { throw new java.sql.SQLDataException("NOT THE RIGHT NUMBER OF ARGS FOR MIMIRCAST, EXPECTED 2 IN FORM OF MIMIRCAST(COLUMN,TYPE)") }
      try {
//        println("Input: " + value_text(0) + " : " + value_text(1))
        val t = Type.toSQLiteType(value_int(1))
//        println("TYPE CASTED: "+t)
        val v = value_text(0)
        logger.trace(s"Casting $v as $t")
        t match {
          case TInt() =>
            value_type(0) match {
              case SQLiteCompat.INTEGER => result(value_int(0))
              case SQLiteCompat.FLOAT   => result(value_double(0).toInt)
              case SQLiteCompat.TEXT
                 | SQLiteCompat.BLOB    => result(java.lang.Long.parseLong(value_text(0)))
              case SQLiteCompat.NULL    => result()
            }
          case TFloat() =>
            value_type(0) match {
              case SQLiteCompat.INTEGER => result(value_int(0).toDouble)
              case SQLiteCompat.FLOAT   => result(value_double(0))
              case SQLiteCompat.TEXT
                 | SQLiteCompat.BLOB    => result(java.lang.Double.parseDouble(value_text(0)))
              case SQLiteCompat.NULL    => result()
            }
          case TString() | TRowId() | TDate() | TTimestamp() =>
            result(value_text(0))

          case TUser(name) =>
            val v:String = value_text(0)
            if(v != null) {
              Type.rootType(t) match {
                case TRowId() =>
                  result(value_text(0))
                case TString() | TDate() | TTimestamp() =>
                  val txt = value_text(0)
                  if(TypeRegistry.matches(name, txt)){
                    result(value_text(0))
                  } else {
                    result()
                  }
                case TInt() | TBool() =>
                  if(TypeRegistry.matches(name, value_text(0))){
                    result(value_int(0))
                  } else {
                    result()
                  }
                case TFloat() =>
                  if(TypeRegistry.matches(name, value_text(0))){
                    result(value_double(0))
                  } else {
                    result()
                  }
                case TAny() =>
                  if(TypeRegistry.matches(name, value_text(0))){
                    result(value_text(0))
                  } else {
                    result()
                  }
                case TUser(_) | TType() =>
                  throw new Exception("In SQLiteCompat expected natural type but got: " + Type.rootType(t).toString())
              }
            }
            else{
              result()
            }

          case _ =>
            result("I assume that you put something other than a number in, this functions works like, MIMIRCAST(column,type), the types are int values, 1 is int, 2 is double, 3 is string, and 5 is null, so MIMIRCAST(COL,1) is casting column 1 to int")
            // throw new java.sql.SQLDataException("Well here we are, I'm not sure really what went wrong but it happened in MIMIRCAST, maybe it was a type, good luck")
        }

      } catch {
        case _:TypeException => result()
        case _:NumberFormatException => result()
      }
    }
}

object GroupAnd extends org.sqlite.Function.Aggregate {
  var agg = true

  @Override
  def xStep(): Unit = {
    agg = agg && (value_int(0) != 0)
  }

  def xFinal(): Unit = {
    result(if(agg){ 1 } else { 0 })
  }
}

object GroupOr extends org.sqlite.Function.Aggregate {
  var agg = false

  @Override
  def xStep(): Unit = {
    agg = agg || (value_int(0) != 0)
  }

  def xFinal(): Unit = {
    result(if(agg){ 1 } else { 0 })
  }
}

object GroupBitwiseAnd extends org.sqlite.Function.Aggregate {
  var agg:Long = 0xffffffffffffffffl

  @Override
  def xStep(): Unit = {
    agg = agg & value_int(0)
  }

  def xFinal(): Unit = {
    result(agg)
  }
}

object GroupBitwiseOr extends org.sqlite.Function.Aggregate {
  var agg:Long = 0

  @Override
  def xStep(): Unit = {
    agg = agg | value_int(0)
  }

  def xFinal(): Unit = {
    result(agg)
  }
}

object OtherTest extends org.sqlite.Function {
  @Override
  def xFunc(): Unit = {
    try {
      result(8000);
    } catch {
      case _: java.sql.SQLDataException => throw new java.sql.SQLDataException();
    }
  }
}

object First extends org.sqlite.Function.Aggregate {
  var firstVal: String = null;
  @Override
  def xStep(): Unit = {
    if(firstVal == null){ firstVal = value_text(0); }
  }
  def xFinal(): Unit = {
    if(firstVal == null){ result(); } else { result(firstVal); }
  }
}

object FirstInt extends org.sqlite.Function.Aggregate {
  var firstVal: Int = 0
  var empty = true

  @Override
  def xStep(): Unit = {
    if(empty){ 
      if(value_type(0) != SQLiteCompat.NULL) {
        firstVal = value_int(0) 
        empty = false
      }
    }
  }
  def xFinal(): Unit = {
    if(empty){ result() } else { result(firstVal) }
  }
}

object FirstFloat extends org.sqlite.Function.Aggregate {
  var firstVal: Double = 0.0
  var empty = true

  @Override
  def xStep(): Unit = {
    if(empty){ 
      if(value_type(0) != SQLiteCompat.NULL) {
        firstVal = value_double(0) 
        empty = false 
      }
    }
  }
  def xFinal(): Unit = {
    if(empty){ result() } else { result(firstVal) }
  }
}

object AggTest extends org.sqlite.Function.Aggregate {

  var total = 0
  @Override
  def xStep(): Unit ={
    total = total + value_int(0)
  }

  def xFinal(): Unit ={
    result(total)
  }
}

object StdDev extends org.sqlite.Function.Aggregate {

   var m = 0.0
   var s = 0.0
   var k = 1

   @Override
   def xStep(): Unit ={
        if(value_type(0) != SQLiteCompat.NULL) {
          val value = value_double(0)
          val tM = m
          m += (value - tM) / k
          s += (value - tM) * (value - m)
          k += 1
        }
   }

   override def xFinal(): Unit ={
       //println(s"sdev - xfinal: $k, $s, $m") 
       if(k >= 3)
            result(math.sqrt(s / (k-2)))
        else
            result(0)
    }
}

object Max extends org.sqlite.Function.Aggregate {

  var theVal: Double = 0.0
  var empty = true

  @Override
  def xStep(): Unit = {
    if(value_type(0) != SQLiteCompat.NULL) {
      if(theVal < value_double(0) )
        theVal = value_double(0) 
      empty = false 
    }
  }
  def xFinal(): Unit = {
    if(empty){ result() } else { result(theVal) }
  }
}



object JsonExplorerProject extends org.sqlite.Function with LazyLogging {

  /*
    The shape of the json object being output will be the following
    {
      "path" : "pathName will be stored here"
      "typeData" : [{
                      "typeName" : "varchar" // for example
                      "typeCount" : 1
                   }]
    }
    - this is one row, to merge multiple rows add a new type in the type data arrow or increase the count
    - this is an example for one 'column' so the total structure would look like the following if the previous structure is denoted as 's'

    {
      "data" : [s],
      "rowCount" : 1
    }

    - this is so it can be parsed by a json shredder and can support changes in the future
    - during aggregation total count and other things can be added

   */

  override def xFunc(): Unit = {

    val dataList: ListBuffer[AllData] = ListBuffer[AllData]()


    try {
      value_type(0) match {
        case SQLiteCompat.TEXT => {
          val jsonString: String = value_text(0)

          try {
            val jsonMap: java.util.Map[String,AnyRef] = JsonFlattener.flattenAsMap(jsonString) // create a flat map of the json object
            val jsonLeafKeySet: Set[String] = jsonMap.keySet()
            val objectKeySet: ListBuffer[String] = createFullObjectSet(jsonLeafKeySet)

            // preform the type counting
            for (key: String <- jsonLeafKeySet.asScala){ // iterate through each key which can be thought of as a column
              try {
                val jsonType: String = Type.getType(jsonMap.get(key).toString).toString() // returns the type
                val tJson: TypeData = JsonPlay.TypeData(jsonType, 1)
                val dJson: AllData = JsonPlay.AllData(key, Option[Seq[TypeData]](Seq[TypeData]((tJson))), None)
                dataList += dJson
              } catch {
                case e: Exception => // do nothing
              }
            }

            // now look at the objects
            objectKeySet.foreach((objectKey: String) => { // for every object, find the structure
              var objectSeq: ListBuffer[String] = ListBuffer[String]()
              jsonLeafKeySet.asScala.foreach((x: String) => { // for every leaf, if object is in the path then it contains it
                val leafList = pullApart(x)
                if(!x.equals(objectKey)) {
                  if(leafList.contains(objectKey)){ // contains key and it's not the last object
                    objectSeq += x
                  }
                }
              })
//              println(objectSeq)
              val dJson: AllData = JsonPlay.AllData(objectKey,None,Option[Seq[ObjectTracker]](Seq[ObjectTracker](JsonPlay.ObjectTracker(objectSeq.toSeq,1))))
              dataList += dJson
            })
          }
          catch{
            case e: Exception => {
//              println(s"Not of JSON format in Json_Explorer_Project, so null returned: $jsonString")
              result()
            } // is not a proper json format so return null since there's nothing we can do about this right now
          }
          val jsonResult: ExplorerObject = ExplorerObject(dataList,1)
          val output: JsValue = Json.toJson(jsonResult)
//          println(Json.prettyPrint(output))
          result(Json.stringify(output))
        }
        case _ => result() // else return null
      }

    } catch {
      case e: Exception =>
        throw e
        //throw new Exception("Something went wrong in sqlitecompact Json_Explorer_Project")
    }

  }

  def pullApart(s: String): ListBuffer[String] = {
    var returnSet: ListBuffer[String] = ListBuffer[String]()
    val keySplit: Array[String] = s.split("\\.")
    var runningPath: String = ""
    for(i <- 0 until keySplit.size){
      if(i != 0)
        runningPath += "."
      runningPath += keySplit(i)
      if(!returnSet.contains(runningPath))
        returnSet += runningPath
    }
    returnSet
  }

  // returns a set that consists of all possible keys
  def createFullObjectSet(s: java.util.Set[String]): ListBuffer[String] = {
    var returnSet: ListBuffer[String] = ListBuffer[String]()
    s.asScala.map((x: String) => {
      val keySplit: Array[String] = x.split("\\.")
      var runningPath: String = ""
      for(i <- 0 until keySplit.size - 1){ // exclude the last element
        if(i != 0)
          runningPath += "."
        runningPath += keySplit(i)
        if(!returnSet.contains(runningPath))
          returnSet += runningPath
      }
    })
    returnSet
  }
}


object JsonExplorerMerge extends org.sqlite.Function.Aggregate {
  // value is the result and flag is what is returned

  var x = 0
  var rowCount = 0
  // the outer string is the 'column' name, the next map contains a name to be used for merging, this will go through a case class
  var columnMap: Map[String,Map[String,Seq[(Any,Int)]]] = Map[String,Map[String,Seq[(Any,Int)]]]()
  //                 colName    dataName
  @Override
  def xStep(): Unit = {

    rowCount += 1
/*    var i = 0
    for(i <- 0 to 10){
      x += 1
      println(x)
    }
    println(x)
*/
    if(value_type(0) != SQLiteCompat.NULL) { // the value is not null, so merge the JSON objects

      val json: JsValue = Json.parse(value_text(0))
      json.validate[ExplorerObject] match {
        case s: JsSuccess[ExplorerObject] => {
          val row: ExplorerObject = s.get
          val allColumns: Seq[AllData] = row.data
//          println(allColumns)
          val updates: Seq[(String,Map[String,Seq[(Any,Int)]])] = allColumns.map((a: AllData) => {

            val colName: String = a.name
            var resultMap: Map[String,Seq[(Any,Int)]] = Map[String,Seq[(Any,Int)]]()
            a.td match {
              case Some(typeInfo: Seq[TypeData]) =>
                val temp: Seq[(String,Int)] = typeInfo.map((t) => {
                  (t.typeName,t.typeCount)
                })
                resultMap += ("TYPE" -> temp)
              case None =>
            }
            a.ot match {
              case Some(objectInfo: Seq[ObjectTracker]) =>
                val temp: Seq[(Seq[String],Int)] = objectInfo.map((t) => {
                  (t.objectRelationship,t.objectCount)
                })
                resultMap += ("OBJECT" -> temp)
              case None =>

            }
            // now add type map for that column to colMap
            (colName -> resultMap)
          })
          columnMap = updateMap(columnMap, updates)

          // all column information has now been updated
        } // end case Explorer Object
        case e: JsError => // failed, probably not the right shape
      }
    } // end value is not null section
  }

  def updateMap(m: Map[String,Map[String,Seq[(Any,Int)]]], seq: Seq[(String,Map[String,Seq[(Any,Int)]])]): Map[String,Map[String,Seq[(Any,Int)]]] = {
    // merges two maps together effectively, should add to the total map or it should increase the count for repeated objects

    var retMap: Map[String,Map[String,Seq[(Any,Int)]]] = m

      seq.foreach((col) => {

        val colName: String = col._1
        col._2.get("TYPE") match {
          case Some(typeMap) =>
            typeMap.foreach((t) => { // update type information for each type possibility
              val typeName: String = t._1.toString
              val typeCount: Int = t._2
              retMap.get(colName) match {
                case Some(tempMap: Map[String,Seq[(Any,Int)]]) => { // temp map is the map for that column
                  tempMap.get("TYPE") match {
                    case Some(typeSeq) => // types exist in both so need to merge
                      var added = false
                      var updates: ListBuffer[(Any,Int)] = ListBuffer[(Any,Int)]()
                      typeSeq.foreach((tCols) => {
                        val typeInMap: String = tCols._1.toString
                        if(typeInMap.equals(typeName)){
                          added = true
                          updates += Tuple2(typeInMap,typeCount + tCols._2)
                        }
                        else{
                          updates += Tuple2(typeInMap,tCols._2)
                        }
                      })
                      if(!added)
                        updates += Tuple2(typeName,typeCount)
                      val localTempMap = tempMap + ("TYPE" -> updates)
                      retMap += (colName -> localTempMap) // covers the case where TYPE is present but not that type, and also when it should be updated

                    case None => // TYPE is not present for that column so make one
                      var tempSeq: ListBuffer[(Any,Int)] = ListBuffer[(Any,Int)]()
                      tempSeq += Tuple2(typeName,typeCount)
                      retMap += (colName -> (tempMap + ("TYPE" -> tempSeq)))
                  }
                }
                case None => // the main map does not contain this column yet
                  retMap += (colName -> col._2)
              }
            })
          case None => // no need to add type info yet if none exists
        }
        col._2.get("OBJECT") match {
          case Some(typeMap) =>
            typeMap.map((t) => {
              val objectShape: Seq[String] = t._1.asInstanceOf[Seq[String]]
              val objectCount: Int = t._2
              retMap.get(colName) match {
                case Some(tempMap: Map[String,Seq[(Any,Int)]]) => {
                  tempMap.get("OBJECT") match {
                    case Some(objSeq: Seq[(Any,Int)]) =>
                      var added = false
                      val updates: ListBuffer[(Any,Int)] = ListBuffer[(Any,Int)]()
                      objSeq.foreach((o) => {
                        val objectInMap: Seq[String] = o._1.asInstanceOf[Seq[String]]
                        if(objectInMap == objectShape){
                          added = true
                          updates += Tuple2(objectShape,objectCount + o._2)
                        }
                        else{
                          updates += Tuple2(objectInMap,o._2)
                        }
                      })
                      if(!added)
                        updates += Tuple2(objectShape,objectCount)
                      val localTempMap = tempMap + ("OBJECT" -> updates)
                      retMap += (colName -> localTempMap) // covers the case where TYPE is present but not that type, and also when it should be updated

                    case None => // does not contain object, but the column is present
                      var tempSeq: ListBuffer[(Any,Int)] = ListBuffer[(Any,Int)]()
                      tempSeq += Tuple2(objectShape,objectCount)
                      retMap += (colName -> (tempMap + ("OBJECT" -> tempSeq)))
                  }
                }
                case None => // column not present so add the default
                  retMap += (colName -> col._2)
              }
            })
          case None => // there is no object so no need to add one
        }
    })
    retMap
  }

  def format(m: Map[String,Map[String,Seq[(Any,Int)]]],count: Int): ExplorerObject = {
    // this is the function that converts the map into the json structure for output
    val allColumns: Seq[AllData] =  m.map((cols) => {
      val colName: String = cols._1
      var typeData: Option[Seq[TypeData]] = None
      var objectData: Option[Seq[ObjectTracker]] = None
      val dataMap: Map[String,Seq[(Any,Int)]] = cols._2
      dataMap.get("TYPE") match {
        case Some(td) =>
          val temp: Seq[TypeData] = td.map((t) => {
            JsonPlay.TypeData(t._1.toString,t._2)
          }).toSeq
          typeData = Option[Seq[TypeData]](temp)

        case None =>
      }
      dataMap.get("OBJECT") match {
        case Some(od) =>
          val temp: Seq[ObjectTracker] = od.map((obj) => {
            JsonPlay.ObjectTracker(obj._1.asInstanceOf[Seq[String]],obj._2)
          }).toSeq
          objectData = Option[Seq[ObjectTracker]](temp)

        case None =>
      }
      JsonPlay.AllData(colName,typeData,objectData)
    }).toSeq
    ExplorerObject(allColumns,count)
  }


  def xFinal(): Unit = {
    // this is what is returned, return a json object with all the encoded information
//    println(x)

    println("FINAL")
    val eo: ExplorerObject = format(columnMap,rowCount)
    val output: JsValue = Json.toJson(eo)

    println(Json.prettyPrint(output))
    result(Json.stringify(output))
  }

}

object JsonClusterProject extends org.sqlite.Function with LazyLogging {

  override def xFunc(): Unit = {
    val combineArrays: Boolean = true // if true, then arrays will be treated as one path

    value_type(0) match {
      case SQLiteCompat.TEXT => {
        val jsonString: String = value_text(0)
        var jsonLeafKeySet: Set[String] = null

        try {
          val jsonMap: java.util.Map[String,AnyRef] = JsonFlattener.flattenAsMap(jsonString) // create a flat map of the json object
          if(combineArrays)
            jsonLeafKeySet = combineArr(jsonMap.keySet())
          else
            jsonLeafKeySet = jsonMap.keySet()
        }
        catch{
          case e: Exception => {
            //              println(s"Not of JSON format in Json_Explorer_Project, so null returned: $jsonString")
            result()
          } // is not a proper json format so return null since there's nothing we can do about this right now
        } // end try catch

        val res: String = jsonLeafKeySet.toString
        result(res.substring(1,res.length - 1)) // remove the set brackets before returning
      }
      case _ => result() // else return null
    }
  }

  def combineArr(paths: Set[String]): Set[String] = {
    var ret: Set[String] = new util.HashSet[String]()
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

}


class JsonClusterAGG extends org.sqlite.Function.Aggregate {

  val rowHolder: ListBuffer[ListBuffer[Int]] = ListBuffer[ListBuffer[Int]]() // Seq of feature vectors
  var totalSchema: ListBuffer[String] = ListBuffer[String]() // the total schema, used to determine the feature vector

  @Override
  def xStep(): Unit = {
    if(value_type(0) != SQLiteCompat.NULL) { // the value is not null, so merge the JSON objects
      val schema: List[String] = value_text(0).replace("\u0020","").split(",").toList
      val featureVector: ListBuffer[Int] = ListBuffer[Int]()
      schema.foreach((v: String) => {
        if(!totalSchema.contains(v))
          totalSchema += v
      })
      totalSchema.foreach((v) => {
        if(schema.contains(v))
          featureVector += 1
        else
          featureVector += 0
      })
      rowHolder += featureVector
    } // end value is not null section
  }

  def xFinal(): Unit = {
    // rowHolder holds the feature vectors for each row, iterate through this
    // total Schema is the maximum schema for the json dataset, this contains an order with all known paths
    result("DONE")
  }

}