package mimir.algebra

import java.sql.SQLException

/**
 * An enum class defining the type of primitive-valued expressions
 * (e.g., integers, floats, strings, etc...)
 */
object Type extends Enumeration {
  /**
   * The base type of the enum.  Type.T is an instance of Type
   */
  type T = Value
  /**
   * The enum values themselves
   */
  val TInt, TFloat, TDate, TString, TBool, TRowId, TType, TBlob, TAny = Value

  /**
   * Convert a type to a SQL-friendly name
   */
  def toString(t: T) = t match {
    case TInt => "int"
    case TFloat => "real"
    case TDate => "date"
    case TString => "varchar"
    case TBool => "bool"
    case TRowId => "rowid"
    case TType => "type"
    case TBlob => "blob"
    case TAny => "any"//throw new SQLException("Unable to produce string of type TAny");
  }

  def toStringPrimitive(t: T) = StringPrimitive(toString(t))

  /**
   * Convert a type from a SQL-friendly name
   */
  def fromString(t: String) = t.toLowerCase match {
    case "int"     => Type.TInt
    case "integer" => Type.TInt
    case "float"   => Type.TFloat
    case "decimal" => Type.TFloat
    case "real"    => Type.TFloat
    case "date"    => Type.TDate
    case "varchar" => Type.TString
    case "char"    => Type.TString
    case "string"  => Type.TString
    case "bool"    => Type.TBool
    case "rowid"   => Type.TRowId
    case "type"    => Type.TType
    case "blob"    => Type.TBlob
    case _ =>  throw new SQLException("Invalid Type '" + t + "'");
  }

  def fromStringPrimitive(t: StringPrimitive) = fromString(t.asString)
}