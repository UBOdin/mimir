package mimir.models;

import scala.util.Random

import mimir.algebra._
import mimir.util._
import mimir.Database

/**
 * A model representing a key-repair choice.
 * 
 * The index is ignored.
 * The one argument is a value for the key.  
 * The return value is an integer identifying the ordinal position of the selected value, starting with 0.
 */
@SerialVersionUID(1001L)
class GeocodingModel(override val name: String, addrCols:Seq[Expression], source: Operator) 
  extends Model(name) 
  with Serializable
  with NeedsReconnectToDatabase
  with ModelCache
  with SourcedFeedback
{
  
  
  val latlonLabel = Seq("Latitude", "Longitude")
  val geogoderLabel = Map("GOOGLE" -> "Google", "OSM" -> "Open Streets")
  
  @transient var db: Database = null
  
  def getCacheKey(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue] ) : String = {
    args(0).asString
  }
  
   def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue] ) : String = {
     s"${idx}_${args(0).asString}"
   }
  
  def argTypes(idx: Int) = {
      Seq(TRowId()).union(addrCols.map(_ => TString()))
  }
  def varType(idx: Int, args: Seq[Type]) = TFloat()
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ) = {
    val rowid = RowIdPrimitive(args(0).asString)
    getFeedback(idx, args) match {
      case Some(v) => v.asInstanceOf[PrimitiveValue]
      case None => {
        getCache(idx, args, hints) match {
          case Some(v) => v.asInstanceOf[FloatPrimitive]
          case None => {
            val houseNumber = args(1).asString
            val streetName = args(2).asString
            val city = args(3).asString
            val state = args(4).asString
            val geocoder = args(5).asString
            val (url, latPath, lonPath) = geocoder match {
              case "GOOGLE" => (s"https://maps.googleapis.com/maps/api/geocode/json?address=${s"$houseNumber+${streetName.replaceAll(" ", "+")},+${city.replaceAll(" ", "+")},+$state".replaceAll("\\+\\+", "+")}&key=AIzaSyAKc9sTF-pVezJY8-Dkuvw07v1tdYIKGHk", ".results[0].geometry.location.lat", ".results[0].geometry.location.lng")
              case "OSM" | _ => (s"http://52.0.26.255/?format=json&street=$houseNumber $streetName&city=$city&state=$state", "[0].lat", "[0].lon")
            }
            try {
                val geoRes = HTTPUtils.getJson(url)
                val glat = JsonUtils.seekPath( geoRes, latPath).toString().replaceAll("\"", "").toDouble
                val glon = JsonUtils.seekPath( geoRes, lonPath).toString().replaceAll("\"", "").toDouble
                val geocacheEntry = (FloatPrimitive(glat), FloatPrimitive(glon))
                setCache(0, args, hints, geocacheEntry._1)
                setCache(1, args, hints, geocacheEntry._2)
                geocacheEntry.productElement(idx).asInstanceOf[FloatPrimitive]                
            } catch {
                case ioe: Throwable =>  {
                  println(ioe.toString())
                  NullPrimitive()
                }
            }
          }
        }
      }
      
    }
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = {
    NullPrimitive()
  }
  def reason(idx: Int, args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): String = {
    val rowid = RowIdPrimitive(args(0).asString)
    val houseNumber = args(1).asString
    val streetName = args(2).asString
    val city = args(3).asString
    val state = args(4).asString
    val geocoder = args(5).asString
    getFeedback(idx, args) match {
      case Some(v) =>
        s"${getReasonWho(idx,args)} told me that $houseNumber $streetName, $city, $state has ${latlonLabel(idx)} = ${v.asInstanceOf[PrimitiveValue]} on row $rowid"
      case None => 
        getCache(idx, args, hints) match {
          case Some(v) =>
            s"I used a geocoder (${geogoderLabel(geocoder)}) to determine that $houseNumber $streetName, $city, $state has ${latlonLabel(idx)} = ${v.asInstanceOf[PrimitiveValue]} on row $rowid "
          case x =>
            s"The location of (${geogoderLabel(geocoder)}) to determine that $houseNumber $streetName, $city, $state is unknown"
        }
      }
  }
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = { 
    setFeedback(idx, args, v)
  }
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = {
    hasFeedback(idx, args)
  }
  def hintTypes(idx: Int): Seq[mimir.algebra.Type] = Seq()
   
  def reconnectToDatabase(db: Database) = { 
    this.db = db 
  }
  
}