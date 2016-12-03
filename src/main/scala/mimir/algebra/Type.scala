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
    case "varchar" => TString()
    case "char"    => TString()
    case "string"  => TString()
    case "bool"    => TBool()
    case "rowid"   => TRowId()
    case "type"    => TType()
    case _ =>  throw new SQLException("Invalid Type '" + t + "'");
  }
  override def toString(): String = {
    toString(this)
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
    case _ => {
      val num = i - 8 // 8 because this is the number of native types, if more are added then this number needs to increase
      if(num < 0){
        throw new Exception("This number is not part of the type: num == " + num.toString())
      }
      var location = 0
      var t:Type = null
      TypeList.typeList.foreach((tuple) => {
        if(location == num){
          t = TUser(tuple._1,tuple._2,tuple._3)
        }
        location += 1
      })
      if(t == null){
        throw new Exception("This number is not part of the type: num == " + num.toString())
      }
      t
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
    case TUser(name,regex,sqlType)  => {
      var location = -1
      var temp = 0
      TypeList.typeList.foreach((tuple) => {
        if(tuple._1.equals(name)){
          location = temp + 8 // 8 because of other types, if more native types are added then you need to increase this number
        }
        temp += 1
      })
      if(location == -1){
        throw new Exception("This number is not part of the type")
      }
      location
    }
  }
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
