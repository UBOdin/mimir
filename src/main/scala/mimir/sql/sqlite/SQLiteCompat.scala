package mimir.sql.sqlite

import mimir.algebra._

import com.typesafe.scalalogging.slf4j.LazyLogging

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
  }
}


object MimirCast extends org.sqlite.Function with LazyLogging {


    @Override
    def xFunc(): Unit = { // 1 is int, double is 2, 3 is string, 5 is null
      if (args != 2) { throw new java.sql.SQLDataException("NOT THE RIGHT NUMBER OF ARGS FOR MIMIRCAST, EXPECTED 2 IN FORM OF MIMIRCAST(COLUMN,TYPE)") }
      try {
//        println("Input: " + value_text(0) + " : " + value_text(1))
        val t = TString().toSQLiteType(value_int(1))
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
          case TString() | TRowId() | TDate() =>
            result(value_text(0))

          case TUser(name,regex,sqlType,priorityLevel) =>
            val v:String = value_text(0)
            if(v != null) {
              sqlType match {
                case TRowId() =>
                  result(value_text(0))
                case TString() | TDate() =>
                    if (v.matches(regex)) {
                      result(value_text(0) + "_" + name)
                    }
                    else {
                      result()
                    }
                case TInt() =>
                    if (value_text(0).matches(regex)) {
                      result(value_int(0) + 10000)
                    }
                    else {
                      result()
                    }
                case TFloat() =>
                  result(value_double(0))
                case TAny() =>
                  result()
                case _ =>
                  throw new Exception("In SQLiteCompat expected natural type but got: " + sqlType.toString())
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