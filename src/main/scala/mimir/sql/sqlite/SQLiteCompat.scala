package mimir.sql.sqlite

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

          // try to clean up the json object, might want to replace this later
          var clean = jsonString.replace("\\\\", "") // clean the string of variables that will throw off parsing
          clean = clean.replace("\\\"", "")
          clean = clean.replace("\\n", "")
          clean = clean.replace("\\r", "")
          clean = clean.replace("\n", "")
          clean = clean.replace("\r", "")
          try {
            val jsonMap: java.util.Map[String,AnyRef] = JsonFlattener.flattenAsMap(clean) // create a flat map of the json object
            val jsonMapKeySet: Set[String] = jsonMap.keySet()
            for (key: String <- jsonMapKeySet.asScala){ // iterate through each key which can be thought of as a column
              val jsonType: String = Type.getType(jsonMap.get(key).toString).toString() // returns the
              val tJson: TypeData = JsonPlay.TypeData(jsonType,1)
              val dJson: AllData = JsonPlay.AllData(key,Seq[TypeData](tJson))
              dataList += dJson
            }
          }
          catch{
            case e: Exception => {
              println(s"Not of JSON format in Json_Explorer_Project, so null returned: $jsonString")
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
      case e: Exception => throw new Exception("Something went wrong in sqlitecompact Json_Explorer_Project")
    }

  }
}


object JsonExplorerMerge extends org.sqlite.Function.Aggregate {
  // value is the result and flag is what is returned

  var x = 0
  var rowCount = 0
  var columnMap: Map[String,Map[String,Int]] = Map[String,Map[String,Int]]()

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
          val updates: Seq[(String,Map[String,Int])] = allColumns.map((a: AllData) => {

            val colName: String = a.name
            val typeInfo: Seq[TypeData] = a.td
            val typeSeq: Seq[(String,Int)] = typeInfo.map((t) => {
              (t.typeName,t.typeCount)
            })
            // now add type map for that column to colMap
            (colName -> typeSeq.toMap)
          })
          columnMap = updateMap(columnMap, updates)

          // all column information has now been updated
        } // end case Explorer Object
        case e: JsError => // failed, probably not the right shape
      }
    } // end value is not null section
  }

  def updateMap(m: Map[String,Map[String,Int]], seq: Seq[(String,Map[String,Int])]): Map[String,Map[String,Int]] = {

    var tempMap: Map[String,Map[String,Int]] = m

      seq.foreach((col) => {
      val colName: String = col._1
      col._2.foreach((t) => {
        val typeName: String = t._1
        val typeCount: Int = t._2
        tempMap.get(colName) match {
          case Some(cMap) => {
            cMap.get(typeName) match {
              case Some(tCount: Int) =>
                tempMap += (colName -> Map((typeName -> (tCount + typeCount))))
              case None =>
                tempMap += (colName -> Map((typeName -> typeCount)))
            } // end type map portion
          }
          case None =>
            tempMap += (colName -> Map((typeName -> typeCount))) // no previous enteries so just add to the temp map
        }
      })
    })
    tempMap
  }

  def format(m: Map[String,Map[String,Int]],count: Int): ExplorerObject = {
    val allColumns: Seq[AllData] =  m.map((cols) => {
      val colName: String = cols._1
      val types: Seq[JsonPlay.TypeData] = cols._2.map((t) => {
        JsonPlay.TypeData(t._1,t._2)
      }).toSeq
      JsonPlay.AllData(colName,types)
    }).toSeq
    ExplorerObject(allColumns,count)
  }


  def xFinal(): Unit = {
    // this is what is returned, return a json object with all the encoded information
//    println(x)

    val eo: ExplorerObject = format(columnMap,rowCount)
    val output: JsValue = Json.toJson(eo)

    println(Json.prettyPrint(output))
    result(Json.stringify(output))
  }

}