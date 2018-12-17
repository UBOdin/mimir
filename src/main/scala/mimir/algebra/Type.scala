package mimir.algebra;

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex
import mimir.util.SealedSubclassEnumeration

/**
 * An enum class defining the type of primitive-valued expressions
 * (e.g., integers, floats, strings, etc...)
 */
sealed trait Type

object BaseType {

  def toString(t:BaseType) = 
    t match {
      case TInt()       => "int"
      case TFloat()     => "real"
      case TDate()      => "date"
      case TTimestamp() => "datetime"
      case TString()    => "varchar"
      case TBool()      => "bool"
      case TRowId()     => "rowid"
      case TType()      => "type"
      case TInterval()  => "interval"
      case TAny()       => "any"
    }

  def fromString(t: String):Option[BaseType] = 
    t.toLowerCase match {
      case "int"       => Some(TInt())
      case "integer"   => Some(TInt())
      case "float"     => Some(TFloat())
      case "double"    => Some(TFloat())
      case "decimal"   => Some(TFloat())
      case "real"      => Some(TFloat() )
      case "num"       => Some(TFloat())
      case "date"      => Some(TDate())
      case "datetime"  => Some(TTimestamp())
      case "timestamp" => Some(TTimestamp())
      case "interval"  => Some(TInterval())
      case "varchar"   => Some(TString())
      case "nvarchar"  => Some(TString())
      case "char"      => Some(TString())
      case "string"    => Some(TString())
      case "text"      => Some(TString())
      case "bool"      => Some(TBool())
      case "rowid"     => Some(TRowId())
      case "type"      => Some(TType())
      case ""          => Some(TAny())
      case "any"       => Some(TAny())
      case _           => None
    }

  val tests = Seq[(BaseType,Regex)](
    TInt()   -> "^(\\+|-)?([0-9]+)$".r,
    TFloat() -> "^(\\+|-)?([0-9]*(\\.[0-9]+)?)$".r,
    TDate()  -> "^[0-9]{4}\\-[0-9]{1,2}\\-[0-9]{1,2}$".r,
    TTimestamp() -> "^[0-9]{4}\\-[0-9]{1,2}\\-[0-9]{1,2}\\s+[0-9]{1,2}:[0-9]{1,2}:([0-9]{1,2}|[0-9]{0,2}\\.[0-9]*)$".r,
    TBool()  -> "^(?i:true|false)$".r
  )

  def guessBaseType(s:String): BaseType = {
    tests.find { case (_,regex) => (regex findFirstIn s) != None }
         .map { case (t,_) => t }
         .getOrElse(TString())
  }

  val idTypeOrder = Seq[BaseType](
    TInt(),
    TFloat(),
    TDate(),
    TString(),
    TBool(),
    TRowId(),
    TType(),
    TAny(),
    TTimestamp(),
    TInterval()
  )
}

sealed trait BaseType extends Type
{
  override def toString(): String = {
    BaseType.toString(this)
  }
  val isNumeric = false
}

case class TInt() extends BaseType
{ 
  override val isNumeric = true
}
case class TFloat() extends BaseType
{ 
  override val isNumeric = true
}

case class TDate() extends BaseType
case class TString() extends BaseType
case class TBool() extends BaseType
case class TRowId() extends BaseType
case class TType() extends BaseType
case class TTimestamp() extends BaseType
case class TInterval() extends BaseType
case class TAny() extends BaseType

case class TUser(name:String) extends Type
{ 
  override def toString() = name
}



/*
Adding new UserTypes to mimir (TUser Types)
- All new types must be added the TypeList in object TypeList to effect the system
- Parameterized types are now supported. The current parameter is Tuple(name:String, regex:String, sqlType:Type), remember to update Tuple# to the right number in object TypeList
  - name:String is the name of the new type, must be unique
  - regex:String is the regular expression that this new type should match on, if the column is of this type will be determined by the threshold, consider the basic types when making this
  - sqlType:Type this is the underlying type of the new type, example TFloat, TDouble... This is for potential speed up later and to better capture the output

Extending the UserType
- This is if you want to add new functionality to TUser, such as adding a new parameter
These are the files that need to change to extend the TUser
  - mimir.lenses.TypeInference:
    - In class TypeInferenceTypes add the new parameters to TUser
    - In class TypeInferer add the new parameters to TUser
  - mimir.utils.JDBCUtils:
    - changes here are made in the function convert, in the case class TUser. This only needs
  - mimir.algebra.Expression:
    - change TUser parameters to match the new desired parameters
    - change sealed trait Type so that all the instances of TUser match the new parameters
  - mimir.sql.sqlite.SQLiteCompat:
    - update TUser type parameters
  */
// object TypeRegistry {
//   val registeredTypes = Map[String,(Regex,Type)](
//     "tuser"         -> ("USER".r,                              TString()),
//     "tweight"       -> ("KG*".r,                               TString()),
//     "productid"     -> ("^P\\d+$".r,                           TString()),
//     "firecompany"   -> ("^[a-zA-Z]\\d{3}$".r,                  TString()),
//     "zipcode"       -> ("^\\d{5}(?:[-\\s]\\d{4})?$".r,         TString()),
//     "container"     -> ("^[A-Z]{4}[0-9]{7}$".r,                TString()),
//     "carriercode"   -> ("^[A-Z]{4}$".r,                        TString()),
//     "mmsi"          -> ("^MID\\d{6}|0MID\\d{5}|00MID\\{4}$".r, TString()),
//     "billoflanding" -> ("^[A-Z]{8}[0-9]{8}$".r,                TString()),
//     "imo_code"      -> ("^\\d{7}$".r,                          TInt())
//   )
//   val idxType = registeredTypes.keys.toIndexedSeq
//   val typeIdx = idxType.zipWithIndex.toMap

//   val matchers = registeredTypes.toSeq.map( t => (t._2._1, t._1) )

//   def baseType(t: String): Type = registeredTypes(t)._2

//   def matcher(t: String): Regex = registeredTypes(t)._1

//   def matches(t: String, v: String): Boolean =
//     !matcher(t).findFirstIn(v).isEmpty
// }
