package mimir.algebra

/*
 Casts the
 */
object NormalCast {


  def cast(s: String,t: Type): PrimitiveValue = {
    var ret: PrimitiveValue = NullPrimitive().asInstanceOf[PrimitiveValue]
    t match {
      case TFloat() => {
        if(Type.getType(s).isInstanceOf[TFloat])
          ret = FloatPrimitive(s.toDouble).asInstanceOf[PrimitiveValue]
      }
      case TString() => {
        ret = StringPrimitive(s).asInstanceOf[PrimitiveValue]
      }
    }
    ret
  }

/*
  def apply(t: Type, x: String): PrimitiveValue =
    apply(t, StringPrimitive(x))\
  */
}
