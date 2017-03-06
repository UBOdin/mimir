package mimir.sql.sqlite

import mimir.algebra._
import mimir.util.JDBCUtils
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.geotools.referencing.GeodeticCalculator

object SQLiteCompat {

  val INTEGER = 1
  val FLOAT   = 2
  val TEXT    = 3
  val BLOB    = 4
  val NULL    = 5

  def registerFunctions(conn:java.sql.Connection):Unit = {
    org.sqlite.Function.create(conn,"MIMIRCAST", MimirCast)
    org.sqlite.Function.create(conn,"OTHERTEST", OtherTest)
    org.sqlite.Function.create(conn,"AGGTEST", AggTest)
    org.sqlite.Function.create(conn, "BOOLAND", BoolAnd)
    org.sqlite.Function.create(conn, "SQRT", Sqrt)
    org.sqlite.Function.create(conn, "DST", Distance)
    org.sqlite.Function.create(conn, "MINUS", Minus)
  }
  
  def getTableSchema(conn:java.sql.Connection, table: String): Option[List[(String, Type)]] =
  {
    val stmt = conn.createStatement()
    val ret = stmt.executeQuery(s"PRAGMA table_info('$table')")
    stmt.closeOnCompletion()
    val result = JDBCUtils.extractAllRows(ret).map( (x) => { 
      val name = x(1).asString.toUpperCase.trim
      val rawType = x(2).asString.trim
      val baseType = rawType.split("\\(")(0).trim
      val inferredType = Type.fromString(baseType)
      
      // println(s"$name -> $rawType -> $baseType -> $inferredType"); 

      (name, inferredType)
    })

    if(result.hasNext){ Some(result.toList) } else { None }
  }
}

object Minus extends org.sqlite.Function with LazyLogging {
  override def xFunc(): Unit = {
    if (args != 2) { throw new java.sql.SQLDataException("NOT THE RIGHT NUMBER OF ARGS FOR SQRT, EXPECTED 2") }
    value_double(0) - value_double(1)
  }
  }

object Distance extends org.sqlite.Function with LazyLogging {
  override def xFunc(): Unit = {
    if (args != 4) { throw new java.sql.SQLDataException("NOT THE RIGHT NUMBER OF ARGS FOR DISTANCE, EXPECTED 4 -- LAT1, LON1, LAT2, LON2") }
    val geodeticCalculator: GeodeticCalculator = new GeodeticCalculator()

    geodeticCalculator.setStartingGeographicPoint(value_double(1), value_double(0))
    geodeticCalculator.setDestinationGeographicPoint(value_double(3), value_double(2))

    Math.abs(geodeticCalculator.getOrthodromicDistance)
  }
}

object Sqrt extends org.sqlite.Function with LazyLogging {
  override def xFunc(): Unit = {
    if (args != 1) { throw new java.sql.SQLDataException("NOT THE RIGHT NUMBER OF ARGS FOR SQRT, EXPECTED 1") }
    Math.sqrt(value_double(0))
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
          case TString() | TRowId() | TDate() | TTimeStamp() =>
            result(value_text(0))

          case TUser(name) =>
            val v:String = value_text(0)
            if(v != null) {
              Type.rootType(t) match {
                case TRowId() =>
                  result(value_text(0))
                case TString() | TDate() | TTimeStamp() =>
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

object BoolAnd extends org.sqlite.Function.Aggregate {
  var isDet = 1

  @Override
  def xStep(): Unit = {
    isDet = isDet & value_int(0)
  }

  def xFinal(): Unit = {
    result(isDet)
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