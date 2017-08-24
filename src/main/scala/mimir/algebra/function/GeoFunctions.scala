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
    fr.register("WEBGEOCODEDISTANCE", 
      {  
        case Seq(lat:PrimitiveValue, lon:PrimitiveValue,StringPrimitive(houseNumber), StringPrimitive(streetName),StringPrimitive(city),StringPrimitive(state),StringPrimitive(geocoder)) => {
          val (url, latPath, lonPath) = geocoder match {
            case "GOOGLE" => (s"https://maps.googleapis.com/maps/api/geocode/json?address=${s"${houseNumber.toDouble.toInt}+${streetName.replaceAll(" ", "+")},+${city.replaceAll(" ", "+")},+$state".replaceAll("\\+\\+", "+")}&key=AIzaSyAKc9sTF-pVezJY8-Dkuvw07v1tdYIKGHk", ".results[0].geometry.location.lat", ".results[0].geometry.location.lng")
            case "OSM" | _ => (s"http://52.0.26.255/?format=json&street=${houseNumber.toDouble.toInt} $streetName&city=$city&state=$state".replaceAll("\\s", "%20"), "[0].lat", "[0].lon")
          }
          try {
              val geoRes = mimir.util.HTTPUtils.getJson(url)
              val glat = mimir.util.JsonUtils.seekPath( geoRes, latPath).toString().replaceAll("\"", "").toDouble
              val glon = mimir.util.JsonUtils.seekPath( geoRes, lonPath).toString().replaceAll("\"", "").toDouble
              FloatPrimitive(org.geotools.referencing.datum.DefaultEllipsoid.WGS84.orthodromicDistance(lon.asDouble, lat.asDouble, glon, glat))
          } catch {
              case ioe: Exception =>  {
                println(ioe.toString())
                NullPrimitive()
              }
          }
        }
      },
      (x: Seq[Type]) => TFloat()
    )
    fr.register("METOLOCDST", 
      {  
        (args) => {
          FloatPrimitive(DefaultEllipsoid.WGS84.orthodromicDistance(
            mimir.sql.sqlite.MeToLocationDistance.myLon.get, //lon1
            mimir.sql.sqlite.MeToLocationDistance.myLat.get, //lat1
            args(0).asDouble, //lon2
            args(1).asDouble  //lat2
          ))
        }
      },
      (args) => {
        (0 until 2).foreach { i => Typechecker.assertNumeric(args(i), Function("METOLOCDST", List())) }; 
        TFloat()
      }
    )
  
  }
}