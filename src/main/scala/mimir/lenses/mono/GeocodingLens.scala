package mimir.lenses.mono

import play.api.libs.json._

import mimir.Database
import mimir.algebra._
import mimir.ctables.Reason
import mimir.lenses._
import mimir.util.{ HTTPUtils, JsonUtils }
import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.exec.spark.MimirSpark

case class GeocodingLensConfig(
  houseNumColumn: Option[ID],
  streetColumn: Option[ID],
  cityColumn: Option[ID],
  stateColumn: Option[ID],
  geocoder: ID,
  latitudeCol: Option[ID],
  longitudeCol: Option[ID]
)
{
  def validate(query: Operator): Seq[String] =
  {
    val cols = query.columnNames.toSet
    Seq(
      "house number" -> houseNumColumn,
      "street" -> streetColumn,
      "city" -> cityColumn,
      "state" -> stateColumn
    ).flatMap { 
      case (_, None)         => None
      case (desc, Some(col)) => 
        if(cols(col)){ None } 
        else {
          Some(s"Invalid $desc '$col'")
        }
    } ++ (if(!GeocodingLens.GEOCODERS.contains(geocoder)){ 
      Seq(s"Invalid geocoder '$geocoder'")
    } else { Seq() })
  }
}

object GeocodingLensConfig
{
  implicit val format: Format[GeocodingLensConfig] = Json.format
}


object GeocodingLens extends MonoLens
{
  val GEOCODERS = Set[ID](
    ID("google"),
    ID("osm")   
  )

  def train(
    db: Database,
    name: ID,
    query: Operator,
    config: JsValue
  ): JsValue = 
  {
    val geocodingConfig = config.as[GeocodingLensConfig]
    val errors = geocodingConfig.validate(query)
    if(!errors.isEmpty){
      throw new RAException(s"Invalid configuration: ${errors.mkString(",")}")
    }
    return config
  }

  def view(
    db: Database,
    name: ID,
    query: Operator,
    config: JsValue,
    friendlyName: String
  ): Operator = 
  {
    val geocode = config.as[GeocodingLensConfig]

    val houseNumColumn = geocode.houseNumColumn.map { Var(_) }.getOrElse { NullPrimitive() } 
    val streetColumn = geocode.streetColumn.map { Var(_) }.getOrElse { NullPrimitive() }
    val cityColumn = geocode.cityColumn.map { Var(_) }.getOrElse { NullPrimitive() }
    val stateColumn = geocode.stateColumn.map { Var(_) }.getOrElse { NullPrimitive() }

    val args = Seq(
        houseNumColumn,
        streetColumn,
        cityColumn,
        stateColumn
      )

    def udf(idx:Int) = 
      Function(
        ID("mimir_geocode_"+geocode.geocoder), 
        IntPrimitive(idx) +: 
          args.map { CastExpression(_, TString()) 
      })
    def withCaveat(idx:Int) = 
      Caveat(name, udf(idx), args, 
        Function(ID("concat"), Seq(
          StringPrimitive("Geocoded '"),
          houseNumColumn,
          StringPrimitive(" "),
          streetColumn,
          StringPrimitive("; "),
          cityColumn,
          StringPrimitive(", "),
          stateColumn,
          StringPrimitive(s"' using ${geocode.geocoder}")
        ))
      )

    val latitudeCol = geocode.latitudeCol.getOrElse(ID("LATITUDE"))
    val longitudeCol = geocode.longitudeCol.getOrElse(ID("LONGITUDE"))

    query.removeColumnsByID( // Remove the columns if they exist already
          latitudeCol, 
          longitudeCol
        ).addColumnsByID(
          latitudeCol -> withCaveat(0), 
          longitudeCol -> withCaveat(1)
        )
  }

  def warnings(
    db: Database, 
    name: ID, 
    query: Operator, 
    cols: Seq[ID],
    configJson: JsValue, 
    friendlyName: String
  ) = Seq[Reason]()
  override def init(db: Database)
  {
    new GoogleGeocoder("AIzaSyAKc9sTF-pVezJY8-Dkuvw07v1tdYIKGHk").register(
      db,
      ID("mimir_geocode_google")
    )
    new OSMGeocoder("http://52.0.26.255").register(
      db,
      ID("mimir_geocode_osm")
    )
  }
}

abstract class Geocoder extends Serializable {
  val cache = 
    scala.collection.mutable.Map[
      (String,String,String,String), // House#, Street, City, State
      Option[(Double, Double)]               // Latitude, Longitude
    ]()

  def apply(args: Seq[PrimitiveValue]): PrimitiveValue =
    apply(args(0), args(1), args(2), args(3), args(4))
  def apply(latOrLong: PrimitiveValue, house: PrimitiveValue, street: PrimitiveValue, city: PrimitiveValue, state: PrimitiveValue): PrimitiveValue =
  {
    val tuple = (house.asString, street.asString, city.asString, state.asString)
    val result = 
      cache.getOrElseUpdate( 
        tuple, 
        { locate(tuple._1, tuple._2, tuple._3, tuple._4) }
      )
    val selected: Double =
      if(latOrLong.asLong == 0){ result.map { _._1 }.getOrElse( 0.0 ) }
      else {                     result.map { _._2 }.getOrElse( 0.0 ) }
    
    return FloatPrimitive(selected)
  }

  def register(db: Database, name: ID)
  {
    db.functions.register(
      name, 
      apply(_:Seq[PrimitiveValue]),
      (t:Seq[Type]) => t match {
        case Seq(TInt(), _, _, _, _) => TFloat()
        case _ => throw new RAException("Geocoders expect 5 arguments")
      }
    )
  }

  def locate(house: String, street: String, city: String, state: String): Option[(Double,Double)]

}

abstract class WebJsonGeocoder(latPath: String, lonPath: String) 
  extends Geocoder
  with LazyLogging
{
  def locate(house: String, street: String, city: String, state: String): Option[(Double,Double)] =
  {
    val actualUrl = url(house, street, city, state)
    try {
      val json = Json.parse(HTTPUtils.get(actualUrl))
      val latitude = JsonUtils.seekPath( 
                        json, 
                        latPath
                      ).toString().replaceAll("\"", "").toDouble
      val longitude = JsonUtils.seekPath( 
                        json, 
                        lonPath
                      ).toString().replaceAll("\"", "").toDouble
      return Some( (latitude, longitude) )
    } catch {
      case ioe: Throwable =>  {
        logger.error(s"Exception with Geocoding Request: $actualUrl", ioe)
        None
      }
    }
  }

  def url(house: String, street: String, city: String, state: String): String
}

class GoogleGeocoder(apiKey: String) extends WebJsonGeocoder(
  ".results[0].geometry.location.lat", 
  ".results[0].geometry.location.lng"
)
{
  def url(house: String, street: String, city: String, state: String) =
    s"https://maps.googleapis.com/maps/api/geocode/json?address=${s"$house+${street.replaceAll(" ", "+")},+${city.replaceAll(" ", "+")},+$state".replaceAll("\\+\\+", "+")}&key=$apiKey"
}

class OSMGeocoder(hostURL: String) extends WebJsonGeocoder(
  "[0].lat", 
  "[0].lon"
)
{
  def url(house: String, street: String, city: String, state: String) =
    s"$hostURL/?format=json&street=$house%20$street&city=$city&state=$state"
}
