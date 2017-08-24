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
@SerialVersionUID(1000L)
class GeocodingModel(override val name: String, addrCols:Seq[Expression], source: Operator) 
  extends Model(name) 
  with Serializable
  with NeedsReconnectToDatabase
  with FiniteDiscreteDomain
{
  
  val feedback = scala.collection.mutable.Map[String,(PrimitiveValue,PrimitiveValue)]()
  val geocache = scala.collection.mutable.Map[String,(PrimitiveValue,PrimitiveValue)]()
 
  val latlonLabel = Seq("Latitude", "Longitude")
  val geogoderLabel = Map("GOOGLE" -> "Google", "OSM" -> "Open Streets")
  
  @transient var db: Database = null
  
  
  def argTypes(idx: Int) = {
      Seq(TRowId()).union(addrCols.map(_ => TString()))
  }
  def varType(idx: Int, args: Seq[Type]) = TFloat()
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ) = {
    val rowid = RowIdPrimitive(args(0).asString)
    feedback.get(rowid.asString) match {
      case Some(v) => v.productElement(idx).asInstanceOf[PrimitiveValue]
      case None => {
        geocache.get(rowid.asString) match {
          case Some(v) => v.productElement(idx).asInstanceOf[PrimitiveValue]
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
                geocache(rowid.asString) = geocacheEntry
                geocacheEntry.productElement(idx).asInstanceOf[PrimitiveValue]                
            } catch {
                case ioe: Exception =>  {
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
    feedback.get(rowid.asString) match {
      case Some(v) =>
        s"You told me that $houseNumber $streetName, $city, $state has ${latlonLabel(idx)} = ${v.productElement(idx).asInstanceOf[PrimitiveValue]} on row $rowid"
      case None => 
        geocache.get(rowid.asString) match {
          case Some(v) =>
            s"I used a geocoder (${geogoderLabel(geocoder)}) to determine that $houseNumber $streetName, $city, $state has ${latlonLabel(idx)} = ${v.productElement(idx).asInstanceOf[PrimitiveValue]} on row $rowid "
          case x =>
            s"The location of (${geogoderLabel(geocoder)}) to determine that $houseNumber $streetName, $city, $state is unknown"
        }
      }
  }
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = { 
    val rowid = args(0).asString
    feedback.get(rowid) match {
      case Some(ev) =>
        if(idx == 0)
          feedback(rowid) = (v, ev._2)
        else
          feedback(rowid) = (ev._1, v)
      case None => 
        if(idx == 0)
          feedback(rowid) = (v, NullPrimitive())
        else
          feedback(rowid) = (NullPrimitive(), v)
    }
  }
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = {
    feedback.contains(args(0).asString) && feedback(args(0).asString)._1 != NullPrimitive() && feedback(args(0).asString)._2 != NullPrimitive()
  }
  def hintTypes(idx: Int): Seq[mimir.algebra.Type] = Seq(TAny())
   
  
  def getDomain(idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]): Seq[(PrimitiveValue,Double)] = Seq()
  
  def reconnectToDatabase(db: Database) = { 
    this.db = db 
  }
  
}