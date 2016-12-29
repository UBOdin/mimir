package mimir.algebra;

import scala.collection.mutable.ListBuffer

case class TypeException(found: Type, expected: Type, 
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
sealed trait Type
{
  override def toString(): String = {
    Type.toString(this)
  }
}

object Type {
  def toString(t:Type) = t match {
    case TInt() => "int"
    case TFloat() => "real"
    case TDate() => "date"
    case TTimeStamp() => "datetime"
    case TString() => "varchar"
    case TBool() => "bool"
    case TRowId() => "rowid"
    case TType() => "type"
    case TAny() => "any"
    case TUser(name,regex,sqlType) => name
  }
  def fromString(t: String) = t.toLowerCase match {
    case "int"     => TInt()
    case "integer" => TInt()
    case "float"   => TFloat()
    case "decimal" => TFloat()
    case "real"    => TFloat()
    case "date"    => TDate()
    case "datetime" => TTimeStamp()
    case "varchar" => TString()
    case "nvarchar" => TString()
    case "char"    => TString()
    case "string"  => TString()
    case "text"    => TString()
    case "bool"    => TBool()
    case "rowid"   => TRowId()
    case "type"    => TType()
    case x         => {
      TypeRegistry.typeList.get(x) match {
        case Some((regex, baseType)) => TUser(x, regex, baseType)
        case None => throw new RAException("Invalid Type '" + t + "'");
      }
    }
  }
  def toSQLiteType(i:Int) = i match {
    case 0 => TInt()
    case 1 => TFloat()
    case 2 => TDate()
    case 3 => TString()
    case 4 => TBool()
    case 5 => TRowId()
    case 6 => TType()
    case 7 => TAny()
    case 8 => TTimeStamp()
    case _ => {
      // 9 because this is the number of native types, if more are added then this number needs to increase
      val name = TypeRegistry.idxType(i-9)
      val record = TypeRegistry.typeList(name)
      TUser(name,record._1,record._2)
    }
  }
  def id(t:Type) = t match {
    case TInt() => 0
    case TFloat() => 1
    case TDate() => 2
    case TString() => 3
    case TBool() => 4
    case TRowId() => 5
    case TType() => 6
    case TAny() => 7
    case TTimeStamp() => 8
    case TUser(name,regex,sqlType)  => 
      // 9 because this is the number of native types, if more are added then this number needs to increase
      TypeRegistry.typeIdx(name.toLowerCase)+9
  }

  val tests = Map[Type,String](
    TInt()   -> "(\\+|-)?([0-9]+)",               
    TFloat() -> "(\\+|-)?([0-9]*(\\.[0-9]+)?)",   
    TDate()  -> "[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}",
    TTimeStamp()  -> "[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}\\ \\[0-9]{2}\\:[0-9]{2}\\:[0-9]{2}",
    TBool()  -> "(?i:true|false)"
  )
}

case class TInt() extends Type
case class TFloat() extends Type
case class TDate() extends Type
case class TString() extends Type
case class TBool() extends Type
case class TRowId() extends Type
case class TType() extends Type
case class TAny() extends Type
case class TUser(name:String,regex:String,sqlType:Type) extends Type
case class TTimeStamp() extends Type



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
object TypeRegistry {
  val typeList = Map[String,(String,Type)](
    "tuser"         -> ("USER",                            TString()),
    "tweight"       -> ("KG*",                             TString()),
    "productid"     -> ("P\\d+",                           TString()),
    "firecompany"   -> ("^[a-zA-Z]\\d{3}$",                TString()),
    "zipcode"       -> ("^\\d{5}(?:[-\\s]\\d{4})?$",       TInt()),
    "container"     -> ("[A-Z]{4}[0-9]{7}",                TString()),
    "carriercode"   -> ("[A-Z]{4}",                        TString()),
    "mmsi"          -> ("MID\\d{6}|0MID\\d{5}|00MID\\{4}", TString()),
    "billoflanding" -> ("[A-Z]{8}[0-9]{8}",                TString())
  )
  val idxType = typeList.keys.toIndexedSeq
  val typeIdx = idxType.zipWithIndex.toMap
}
