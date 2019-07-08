package mimir.exec.spark.udf


class MimirUDF {
  def getPrimitive(t:Type, value:Any) = value match {
    case null => NullPrimitive()
    case _ => t match {
      //case TInt() => IntPrimitive(value.asInstanceOf[Long])
      case TInt() => IntPrimitive(value.asInstanceOf[Long])
      case TFloat() => FloatPrimitive(value.asInstanceOf[Double])
      case TDate() => SparkUtils.convertDate(value.asInstanceOf[Date])
      case TTimestamp() => SparkUtils.convertTimestamp(value.asInstanceOf[Timestamp])
      case TString() => StringPrimitive(value.asInstanceOf[String])
      case TBool() => BoolPrimitive(value.asInstanceOf[Boolean])
      case TRowId() => RowIdPrimitive(value.asInstanceOf[String])
      case TType() => TypePrimitive(Type.fromString(value.asInstanceOf[String]))
      //case TAny() => NullPrimitive()
      //case TUser(name) => name.toLowerCase
      //case TInterval() => Primitive(value.asInstanceOf[Long])
      case _ => StringPrimitive(value.asInstanceOf[String])
    }
  }
  def getNative(primitive : PrimitiveValue) : AnyRef = 
    primitive match {
      case NullPrimitive() => null
      case RowIdPrimitive(s) => s
      case StringPrimitive(s) => s
      case IntPrimitive(i) => new java.lang.Long(i)
      case FloatPrimitive(f) => new java.lang.Double(f)
      case BoolPrimitive(b) => new java.lang.Boolean(b)
      case ts@TimestampPrimitive(y,m,d,h,mm,s,ms) => SparkUtils.convertTimestamp(ts)
      case dt@DatePrimitive(y,m,d) => SparkUtils.convertDate(dt)
      case x =>  x.asString
    }
  def getStructType(datatypes:Seq[DataType]): StructType = {
    StructType(datatypes.map(dti => StructField("", OperatorTranslation.getInternalSparkType(dti), true)))
  }
}
