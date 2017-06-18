package mimir.algebra.function;

import org.geotools.referencing.datum.DefaultEllipsoid
import org.joda.time.DateTime
import mimir.algebra._

object GeoFunctions
{

  def register(fr: FunctionRegistry)
  {
    fr.registerExpr("DISTANCE", List("A", "B"), 
      Function("SQRT", List(
        Arithmetic(Arith.Add,
          Arithmetic(Arith.Mult, Var("A"), Var("A")),
          Arithmetic(Arith.Mult, Var("B"), Var("B"))
      ))))

    fr.register(
      "DST",
      (args) => {
        FloatPrimitive(DefaultEllipsoid.WGS84.orthodromicDistance(
          args(0).asDouble, //lon1
          args(1).asDouble, //lat1
          args(2).asDouble, //lon2
          args(3).asDouble  //lat2
        ))
      },
      (args) => {
        (0 until 4).foreach { i => Typechecker.assertNumeric(args(i), Function("DST", List())) }; 
        TFloat()
      }
    )
    fr.register(
      "SPEED",
      (args) => {
        val distance: Double = args(0).asDouble
        val startingDate: DateTime = args(1).asDateTime
        val endingDate: DateTime =
          args(2) match {
            case NullPrimitive() => new DateTime()
            case x => x.asDateTime
          }

        val numberOfHours: Long = Math.abs(endingDate.getMillis - startingDate.getMillis) / 1000 / 60 / 60

        FloatPrimitive(distance / 1000 / numberOfHours) // kmph
      },
      (_) => TFloat()
    )
  }

}