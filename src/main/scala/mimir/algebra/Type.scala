package mimir.algebra;

case class TypeException(found: Type.T, expected: Type.T, 
                    detail:String, context:Option[Expression] = None) 
   extends Exception(
    "Type Mismatch ["+detail+
     "]: found "+found.toString+
    ", but expected "+expected.toString+(
      context match {
        case None => ""
        case Some(expr) => " "+expr.toString
      }
    )
);

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
    case "text"    => Type.TString
    case "bool"    => Type.TBool
    case "rowid"   => Type.TRowId
    case "blob"    => Type.TBlob
    case "type"    => Type.TType
    case "tint"    => Type.TInt
    case "tfloat"  => Type.TFloat
    case "tdate"   => Type.TDate
    case "tstring" => Type.TString
    case "tbool"   => Type.TBool
    case "trowid"  => Type.TRowId
    case "ttype"   => Type.TType
    case "tblob"   => Type.TType
    case "any"     => Type.TAny
    case _ =>  throw new SQLException("Invalid Type '" + t + "'");
  }

  def fromStringPrimitive(t: StringPrimitive) = fromString(t.asString)
}
