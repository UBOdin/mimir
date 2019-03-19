package mimir.backend.sqlite;

import java.sql.SQLException

import mimir.algebra._
import mimir.algebra.Type._
import mimir.util._

abstract class MimirFunction extends org.sqlite.Function
{
  def value_mimir(idx: Int): PrimitiveValue =
    value_mimir(idx, TAny())

  def value_mimir(idx: Int, t:Type): PrimitiveValue =
  {
    if(value_type(idx) == SQLiteCompat.NULL){ NullPrimitive() }
    else { t match {
      case TInt()    => IntPrimitive(value_int(idx))
      case TFloat()  => FloatPrimitive(value_double(idx))
      case TAny()    => 
        value_type(idx) match {
          case SQLiteCompat.INTEGER => IntPrimitive(value_int(idx))
          case SQLiteCompat.FLOAT   => FloatPrimitive(value_double(idx))
          case SQLiteCompat.TEXT
             | SQLiteCompat.BLOB    => StringPrimitive(value_text(idx))
        }
      case _       => TextUtils.parsePrimitive(t, value_text(idx))
    }}
  }

  def return_mimir(p: PrimitiveValue): Unit =
  {
    p match {
      case IntPrimitive(i)      => result(i)
      case FloatPrimitive(f)    => result(f)
      case StringPrimitive(s)   => result(s)
      case d:DatePrimitive      => result(d.asString)
      case BoolPrimitive(true)  => result(1)
      case BoolPrimitive(false) => result(0)
      case RowIdPrimitive(r)    => result(r)
      case t:TimestampPrimitive => result(t.asString)
      case i:IntervalPrimitive => result(i.asString)
      case TypePrimitive(t)     => result(Type.toString(t))
      case NullPrimitive()      => result()
    }
  }
}

abstract class SimpleMimirFunction(argTypes: List[Type]) extends MimirFunction
{
  def apply(args: List[PrimitiveValue]): PrimitiveValue

  override def xFunc(): Unit = {
    return_mimir(
      apply(
        argTypes.zipWithIndex.map( { case (t, i) => value_mimir(i, t) } )
      )
    )
  }
}
