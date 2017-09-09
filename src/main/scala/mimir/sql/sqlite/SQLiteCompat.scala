package mimir.sql.sqlite

import mimir.algebra._
import mimir.provenance._
import mimir.util.JDBCUtils
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.geotools.referencing.datum.DefaultEllipsoid
import org.joda.time.DateTime
import com.typesafe.scalalogging.slf4j.LazyLogging

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
    //val endingDate: DateTime = DateTime.parse(endingDateText)
    var endingDate: DateTime = new DateTime
    if(endingDateText != null) {
      endingDate = DateTime.parse(endingDateText)
    }
    else {
      endingDate = startingDate.plusDays(1)
    }

    val numberOfHours: Long = Math.abs(endingDate.getMillis - startingDate.getMillis) / 1000 / 60 / 60

    result(distance / 1000.0 / numberOfHours) // kmph
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
    def xFunc(): Unit = { 
      if (args != 2) { throw new java.sql.SQLDataException("NOT THE RIGHT NUMBER OF ARGS FOR MIMIRCAST, EXPECTED 2 IN FORM OF MIMIRCAST(COLUMN,TYPE)") }
      try {
        val t = Type.toSQLiteType(value_int(1))
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
                case TString() | TDate() | TTimestamp() | TInterval()  =>
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
