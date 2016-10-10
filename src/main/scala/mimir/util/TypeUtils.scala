package mimir.util

import mimir.algebra._


object TypeUtils {

  val INT = "INTEGER"
  val FLOAT = "REAL"
  val BOOL = "BOOLEAN"
  val STRING = "VARCHAR"
  val DATE = "DATE"
  val ROWID = "VARCHAR"
  val ANY = "NULL"

  val Types = List(INT, FLOAT, BOOL, STRING, DATE)

  def convert(t: Type): String = {
    t match {
      case TInt() => INT
      case TFloat() => FLOAT
      case TBool() => BOOL
      case TString() => STRING
      case TDate() => DATE
      case TRowId() => ROWID
      case TAny() => ANY
    }
  }

  def convert(s: String): Type = {
    s match {
      case INT => TInt()
      case FLOAT => TFloat()
      case BOOL => TBool()
      case STRING => TString()
      case DATE => TDate()
      case ROWID => TRowId()
      case ANY => TAny()
    }
  }

}
