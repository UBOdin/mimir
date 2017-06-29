package mimir.util

import org.specs2.specification._
import org.specs2.specification.core.Fragments
import org.specs2.mutable._

import mimir.algebra._

object TextUtilsSpec
  extends Specification
{
  
  "TextUtils" should {

    "Parse Primitive Values" >> {

      Fragments.foreach(Seq(

        (TInt(),    "1",       IntPrimitive(1)),
        (TFloat(),  "1.0",     FloatPrimitive(1.0)),
        (TFloat(),  "1",       FloatPrimitive(1.0)),
        (TFloat(),  "1e-2",    FloatPrimitive(0.01)),
        (TBool(),   "YES",     BoolPrimitive(true)),
        (TBool(),   "yes",     BoolPrimitive(true)),
        (TBool(),   "True",    BoolPrimitive(true)),
        (TBool(),   "NO",      BoolPrimitive(false)),
        (TBool(),   "0",       BoolPrimitive(false)),
        (TType(),   "int",     TypePrimitive(TInt())),
        (TType(),   "zipcode", TypePrimitive(TUser("zipcode")))

      )) { case (t, str, v) =>
        s"CAST('$str' AS $t) == $v" >> {
          TextUtils.parsePrimitive(t, str) must be equalTo(v)
        }
      }
    }

    "Parse Dates" >> {

      TextUtils.parseDate("2017-02-12") must be equalTo(DatePrimitive(2017, 2, 12))

    }

    "Parse Timestamps" >> {

      TextUtils.parseTimestamp("2017-02-12 02:12:16") must be equalTo(TimestampPrimitive(2017, 2, 12, 2, 12, 16, 0))
      TextUtils.parseTimestamp("2013-10-07 08:23:19.120") must be equalTo(TimestampPrimitive(2013, 10, 7, 8, 23, 19, 120))
      TextUtils.parseTimestamp("2013-10-07 08:23:19.12") must be equalTo(TimestampPrimitive(2013, 10, 7, 8, 23, 19, 120))
      TextUtils.parseTimestamp("2013-10-07 08:23:19.1") must be equalTo(TimestampPrimitive(2013, 10, 7, 8, 23, 19, 100))
      TextUtils.parseTimestamp("2013-10-07 08:23:19.1201") must be equalTo(TimestampPrimitive(2013, 10, 7, 8, 23, 19, 120))

    }

  }

}