package mimir.util

import mimir.algebra.Type

object TypeUtils {

  val INT = "INTEGER"
  val FLOAT = "REAL"
  val BOOL = "BOOLEAN"
  val STRING = "VARCHAR"
  val DATE = "DATE"
  val ROWID = "VARCHAR"
  val ANY = "NULL"

  val Types = List(INT, FLOAT, BOOL, STRING, DATE)

  def convert(t: Type.T): String = {
    t match {
      case Type.TInt => INT
      case Type.TFloat => FLOAT
      case Type.TBool => BOOL
      case Type.TString => STRING
      case Type.TDate => DATE
      case Type.TRowId => ROWID
      case Type.TAny => ANY
    }
  }

  def convert(s: String): Type.T = {
    s match {
      case INT => Type.TInt
      case FLOAT => Type.TFloat
      case BOOL => Type.TBool
      case STRING => Type.TString
      case DATE => Type.TDate
      case ROWID => Type.TRowId
      case ANY => Type.TAny
    }
  }

}
