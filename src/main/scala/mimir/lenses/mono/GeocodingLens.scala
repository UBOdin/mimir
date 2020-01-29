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

  
//   private def makeGeocodeRequest(args: Seq[PrimitiveValue]) : Option[String] = {
//     val houseNumber = args(1) match { case NullPrimitive() => "" ; case x => x.asString }
//     val streetName = args(2) match { case NullPrimitive() => "" ; case x => x.asString }
//     val city = args(3) match { case NullPrimitive() => "" ; case x => x.asString }
//     val state = args(4) match { case NullPrimitive() => "" ; case x => x.asString }
//     val url = geocoder match {
//       case ID("GOOGLE") => (
//       case ID("OSM") | _ => (s"http://52.0.26.255/?format=json&street=$houseNumber%20$streetName&city=$city&state=$state")
//     }
//     try {
//       val geoRes = HTTPUtils.get(url) 
//       setCache(0, args, Seq(), StringPrimitive(geoRes))
//       Some(geoRes)
//     } catch {
//         case ioe: Throwable =>  {
//           logger.error(s"Exception with Geocoding Request: $url", ioe)
//           None
//         }
//     }       
//   }




// import mimir.algebra._
// import mimir.ctables._
// import mimir.util.RandUtils
// import mimir.Database
// import mimir.parser._
// import mimir.models._

// import scala.collection.JavaConversions._
// import scala.util._

// object GeocodingLens {

//   def create(
//     db: Database, 
//     name: ID, 
//     humanReadableName: String,
//     query: Operator, 
//     args:Seq[Expression]
//   ): (Operator, Seq[Model]) =
//   {
//     val operSchema = db.typechecker.schemaOf(query)
//     val schemaMap = operSchema.toMap
    
//     val houseNumColumn = args.flatMap {
//       case Function(ID("house_number"), cols ) => 
//         Some( cols.map { case col:Var => col 
//                          case manVal:StringPrimitive => manVal 
//                          case col => throw new RAException(s"Invalid House Number col argument: $col in GeocodingLens $name (not a column reference)")
//                        } )
//       case _ => None
//     }.toSeq.flatten match {
//       case Seq(vcol@Var(col)) => vcol
//       case Seq(sval@StringPrimitive(x)) => sval
//       case Seq() => StringPrimitive("")
//       case x => throw new RAException(s"Invalid house number argument: $x in GeocodingLens $name (bad type)")
//     }
    
//     val streetColumn = args.flatMap {
//       case Function(ID("street"), cols ) => 
//         Some( cols.map { case col:Var => col 
//                          case manVal:StringPrimitive => manVal 
//                          case col => throw new RAException(s"Invalid street argument: $col in GeocodingLens $name (not a column reference or string value)")
//                        } )
//       case _ => None
//     }.toSeq.flatten match {
//       case Seq(vcol@Var(col)) => vcol
//       case Seq(sval@StringPrimitive(x)) => sval
//       case Seq() => StringPrimitive("")
//       case x => throw new RAException(s"Invalid street argument: $x in GeocodingLens $name (bad type)")
//     }
    
//     val cityColumn = args.flatMap {
//       case Function(ID("city"), cols ) => 
//         Some( cols.map { case col:Var => col 
//                          case manVal:StringPrimitive => manVal  
//                          case col => throw new RAException(s"Invalid City Col argument: $col in GeocodingLens $name (not a column reference)")
//                        } )
//       case _ => None
//     }.toSeq.flatten match {
//       case Seq(vcol@Var(col)) => vcol
//       case Seq(sval@StringPrimitive(x)) => sval
//       case Seq() => StringPrimitive("")
//       case x => throw new RAException(s"Invalid city argument: $x in GeocodingLens $name (bad type)")
//     }
    
//     val stateColumn = args.flatMap {
//       case Function(ID("state"), cols ) => 
//         Some( cols.map { case col:Var => col  
//                          case manVal:StringPrimitive => manVal 
//                          case col => throw new RAException(s"Invalid State Col argument: $col in GeocodingLens $name (not a column reference)")
//                        } )
//       case _ => None
//     }.toSeq.flatten match {
//       case Seq(vcol@Var(col)) => vcol
//       case Seq(sval@StringPrimitive(x)) => sval
//       case Seq() => StringPrimitive("")
//       case x => throw new RAException(s"Invalid state argument: $x in GeocodingLens $name (bad type)")
//     }
    
//     val geocoder = args.flatMap {
//       case Function(ID("geocoder"), cols ) => 
//         Some( cols match { case Seq(Var(ID("GOOGLE"))) | Seq(StringPrimitive("GOOGLE")) => StringPrimitive("GOOGLE")
//                            case Seq(Var(ID("OSM"))) | Seq(StringPrimitive("OSM")) => StringPrimitive("OSM")
//                            case col => StringPrimitive("OSM")
//                        } )
//       case _ => None
//     }.toSeq match {
//       case Seq(x) => x
//       case Seq() => StringPrimitive("OSM")
//       case x => throw new RAException(s"Invalid geocoder argument: $x in GeocodingLens $name (bad type)")
//     }
    
//     val defaultAPIKey = "AIzaSyAKc9sTF-pVezJY8-Dkuvw07v1tdYIKGHk"
//     val apiKey = args.flatMap {
//       case Function(ID("api_key"), cols ) => cols match { 
//         case Seq(StringPrimitive(key)) => Some(key)
//         case x => None
//       } 
//       case _ => None
//     }.toSeq match {
//       case Seq(x) => x
//       case Seq() => defaultAPIKey
//       case x => throw new RAException(s"Invalid api key argument: $x in GeocodingLens $name (bad type)")
//     }
    
//     val resultCols = args.flatMap {
//       case Function(ID("result_columns"), cols:Seq[Var] @unchecked) => Some( cols.map(_.name) )
//       case _ => None
//     }.flatten
    
//     val projectedOutAddrCols = args.flatMap {
//       case Function(ID("hide_addr_columns"), cols:Seq[Var] @unchecked) => Some( cols.map(_.name) )
//       case _ => None
//     }.flatten
    
//     val resultColNames = resultCols.length match {
//       case 0 => Seq(ID("LATITUDE"), ID("LONGITUDE"))
//       case 1 => Seq(resultCols.head, ID("LONGITUDE"))
//       case x => Seq(resultCols(0), resultCols(1))
//     }
    
//     val inCols = Seq(houseNumColumn, streetColumn, cityColumn, stateColumn)
//     val inColsStr = inCols.map(entry => entry match {
//       case StringPrimitive(x) => x
//       case Var(x) => x
//       case x => ???
//     }).mkString("_")
    
    
    
//     val geocodingModel = new GeocodingModel(ID(name,"_GEOCODING_MODEL:"+inColsStr), inCols, ID(geocoder.asString), apiKey, query) 
//     geocodingModel.reconnectToDatabase(db)
    
//     val projectArgs = 
//       query.columnNames.
//         flatMap( col => 
//           if(projectedOutAddrCols.contains(col))
//             None
//           else
//             Some(ProjectArg(col, Var(col)))
//         ).union(Seq( 
//             ProjectArg(resultColNames(0), VGTerm(geocodingModel.name, 0,Seq[Expression](RowIdVar())++inCols, Seq()) ), 
//             ProjectArg(resultColNames(1), VGTerm(geocodingModel.name, 1,Seq[Expression](RowIdVar())++inCols, Seq()) ) 
//         ))

//     return (
//       Project(projectArgs, query),
//       Seq(geocodingModel)
//     )
//   }
// }


// package mimir.models;

// import scala.util.Random

// import mimir.algebra._
// import mimir.util._
// import mimir.Database
// import play.api.libs.json.JsObject
// import play.api.libs.json.JsArray
// import com.typesafe.scalalogging.slf4j.LazyLogging

// /**
//  * A model representing a key-repair choice.
//  * 
//  * The index is ignored.
//  * The one argument is a value for the key.  
//  * The return value is an integer identifying the ordinal position of the selected value, starting with 0.
//  */
// @SerialVersionUID(1002L)
// class GeocodingModel(override val name: ID, addrCols:Seq[Expression], geocoder:ID, apiKey:String, source: Operator) 
//   extends Model(name) 
//   with Serializable
//   with ModelCache
//   with SourcedFeedback
//   with FiniteDiscreteDomain
//   with LazyLogging
// {
  
  
//   val latlonLabel = Seq("Latitude", "Longitude")
//   val (bestGuessResultPathLat, bestGuessResultPathLon) = 
//     Map(
//       ID("GOOGLE") -> (
//         ".results[0].geometry.location.lat", 
//         ".results[0].geometry.location.lng"
//       ), 
//       ID("OSM") -> (
//         "[0].lat", 
//         "[0].lon"
//       )
//     ).get(geocoder).get
//   val geogoderLabel = Map(
//       ID("GOOGLE") -> "Google", 
//       ID("OSM") -> "Open Streets"
//     ).get(geocoder).get
  
//   @transient var db: Database = null
  
//   def getCacheKey(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue] ) : ID = {
//     ID(args(0).asString)
//   }
  
//    def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue] ) : ID = {
//      ID(s"${idx}_${args(0).asString}")
//    }
  
//   def argTypes(idx: Int) = {
//       Seq(TRowId()).union(addrCols.map(_ => TString()))
//   }
//   def varType(idx: Int, args: Seq[Type]) = TFloat()
//   def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ) = {
//     val rowid = RowIdPrimitive(args(0).asString)
//     getFeedback(idx, args) match {
//       case Some(v) => v.asInstanceOf[PrimitiveValue]
//       case None => {
//         (getCache(idx, args, hints) match {
//           case Some(StringPrimitive(v)) => {
//             Some(v)
//           }
//           case Some(x) => None
//           case None => makeGeocodeRequest(args)
//         }) match {
//           case Some(jsonStr) => jsonToPrimitiveValue(idx, jsonStr)
//           case None => NullPrimitive()
//         }
//       }    
//     }
//   }
//   def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = {
//     NullPrimitive()
//   }
//   def reason(idx: Int, args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): String = {
//     val rowid = RowIdPrimitive(args(0).asString)
//     val houseNumber = args(1).asString
//     val streetName = args(2).asString
//     val city = args(3).asString
//     val state = args(4).asString
//     getFeedback(idx, args) match {
//       case Some(v) =>
//         s"${getReasonWho(idx,args)} told me that $houseNumber $streetName, $city, $state has ${latlonLabel(idx)} = ${v.asInstanceOf[PrimitiveValue]} on row $rowid"
//       case None => 
//         getCache(idx, args, hints) match {
//           case Some(StringPrimitive(jsonStr)) => {
//             val v = jsonToPrimitiveValue(idx, jsonStr)
//             s"I used a geocoder (${geogoderLabel}) to determine that $houseNumber $streetName, $city, $state has ${latlonLabel(idx)} = ${v.asInstanceOf[PrimitiveValue]} on row $rowid "
//           }
//           case x =>
//             s"The location of (${geogoderLabel}) to determine that $houseNumber $streetName, $city, $state is unknown"
//         }
//       }
//   }
//   def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = { 
//     setFeedback(idx, args, v)
//   }
//   def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = {
//     hasFeedback(idx, args)
//   }
//   def hintTypes(idx: Int): Seq[mimir.algebra.Type] = Seq()
   
//   def getDomain(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): Seq[(PrimitiveValue,Double)] = 
//   {
//     (getCache(idx, args, hints) match {
//       case Some(StringPrimitive(jsonStr)) => Some(jsonStr)
//       case Some(x) => None
//       case None => makeGeocodeRequest(args) 
//     }) match {
//       case Some(jsonStr) => {
//         val geoJson = play.api.libs.json.Json.parse(jsonStr)
//         geocoder match {
//           case ID("GOOGLE") => {
//             val jsonresults = geoJson.as[JsObject].value("results").as[JsArray]
//             jsonresults.value.map(geoEntry => (
//                 FloatPrimitive(JsonUtils.seekPath( geoEntry, ".geometry.location."+(if(idx==0)"lat"else"lng")).toString().replaceAll("\"", "").toDouble), 
//                 1.0) )
//           }
//           case ID("OSM") => {
//             val jsonresults = geoJson.as[JsArray]
//             jsonresults.value.map(geoEntry => (
//                 FloatPrimitive(JsonUtils.seekPath( geoEntry, if(idx==0)".lat"else".lon").toString().replaceAll("\"", "").toDouble), 
//                 JsonUtils.seekPath( geoEntry, ".importance").toString().replaceAll("\"", "").toDouble) )
//           }
//         }
//       }
//       case None => Seq()
//     }
//   }
  
//   def reconnectToDatabase(db: Database) = { 
//     this.db = db 
//   }

//   def confidence(idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]): Double = {
//     getFeedback(idx, args) match {
//       case Some(v) => 1.0
//       case None =>
//         getCache(idx, args, hints) match {
//           case Some(v) => 1.0
//           case None => 0.0
//         }
//     }
//   }
  
//   private def makeGeocodeRequest(args: Seq[PrimitiveValue]) : Option[String] = {
//     val houseNumber = args(1) match { case NullPrimitive() => "" ; case x => x.asString }
//     val streetName = args(2) match { case NullPrimitive() => "" ; case x => x.asString }
//     val city = args(3) match { case NullPrimitive() => "" ; case x => x.asString }
//     val state = args(4) match { case NullPrimitive() => "" ; case x => x.asString }
//     val url = geocoder match {
//       case ID("GOOGLE") => (s"https://maps.googleapis.com/maps/api/geocode/json?address=${s"$houseNumber+${streetName.replaceAll(" ", "+")},+${city.replaceAll(" ", "+")},+$state".replaceAll("\\+\\+", "+")}&key=$apiKey")
//       case ID("OSM") | _ => (s"http://52.0.26.255/?format=json&street=$houseNumber%20$streetName&city=$city&state=$state")
//     }
//     try {
//       val geoRes = HTTPUtils.get(url) 
//       setCache(0, args, Seq(), StringPrimitive(geoRes))
//       Some(geoRes)
//     } catch {
//         case ioe: Throwable =>  {
//           logger.error(s"Exception with Geocoding Request: $url", ioe)
//           None
//         }
//     }       
//   }

//   private def jsonToPrimitiveValue(idx:Int, jsonStr:String) : PrimitiveValue = {
//     try {
//       val geoJson = play.api.libs.json.Json.parse(jsonStr)
//       FloatPrimitive(idx match {
//         case 0 => JsonUtils.seekPath( geoJson, bestGuessResultPathLat).toString().replaceAll("\"", "").toDouble
//         case 1 => JsonUtils.seekPath( geoJson, bestGuessResultPathLon).toString().replaceAll("\"", "").toDouble
//         case x => throw new Exception(s"idx: $x is not valid for this geocoding model.")
//       })
//     }catch {
//         case ioe: Throwable =>  {
//           // println(ioe.toString())
//           NullPrimitive()
//         }
//     }
//   }
// }
