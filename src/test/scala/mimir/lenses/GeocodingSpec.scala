package mimir.lenses

import java.io._
import mimir._
import mimir.algebra._
import mimir.test._

import org.specs2.specification._
import org.joda.time.DateTime
import java.util.Locale
import mimir.exec.mode.DumpDomain
import play.api.libs.json.JsArray
import play.api.libs.json.JsObject

object GeocodingSpec 
  extends SQLTestSpecification("GeocodingTest") 
  with BeforeAll
{

  def beforeAll = {
    loadCSV("ADDR", new File("test/data/geo.csv"), false, true)
  }
  
  "The Geocoding Lens" should {
    sequential 
    
    "Be able to sucessfully make web requests" >> {
      
      val result = query("""
        SELECT WEBJSON('http://api.geonames.org/timezoneJSON?lat=42.80&lng=-78.89&username=ubodintestcase', '.time') AS NOW_TIME FROM ADDR
      """){ _.map { row => 
        
         row("NOW_TIME").asString
        
      }.toList }.toList
      result(0).replaceAll("\"", "").substring(0, 10) must be equalTo new DateTime().toString("YYYY-MM-dd", Locale.US)
    }
    
    "Be able to create and query geocoding lenses with google geocoder" >> {
 
      update("""
        CREATE LENS GEO_LENS_GOOGLE 
          AS SELECT * FROM ADDR
        WITH GEOCODE(HOUSE_NUMBER(STRNUMBER),STREET(STRNAME),CITY(CITY),STATE(STATE),GEOCODER(GOOGLE))
      """);

      val result = query("""
        SELECT LATITUDE, LONGITUDE FROM GEO_LENS_GOOGLE
      """)(results => results.toList.map( row =>  { 
        (
          row("LATITUDE"), 
          row("LONGITUDE"), 
          row.isColDeterministic("LATITUDE"),
          row.isColDeterministic("LONGITUDE"),
          row.isDeterministic()
        )
       }))
      
      result(0)._3 must be equalTo false
      result(0)._4 must be equalTo false
      result(1)._3 must be equalTo false
      result(1)._4 must be equalTo false
      result(2)._3 must be equalTo false
      result(2)._4 must be equalTo false
      result(3)._3 must be equalTo false
      result(3)._4 must be equalTo false
      result(4)._3 must be equalTo false
      result(4)._4 must be equalTo false
      result(5)._3 must be equalTo false
      result(5)._4 must be equalTo false
      result(6)._3 must be equalTo false
      result(6)._4 must be equalTo false
      
    }
    
    "Be able to create and query geocoding lenses with open streets geocoder" >> {
 
      update("""
        CREATE LENS GEO_LENS_OSM 
          AS SELECT * FROM ADDR
        WITH GEOCODE(HOUSE_NUMBER(STRNUMBER),STREET(STRNAME),CITY(CITY),STATE(STATE),GEOCODER(OSM))
      """);

      val result = query("""
        SELECT LATITUDE, LONGITUDE FROM GEO_LENS_OSM
      """){ _.map { row => 
        (
          row("LATITUDE"), 
          row("LONGITUDE"), 
          row.isColDeterministic("LATITUDE"),
          row.isColDeterministic("LONGITUDE"),
          row.isDeterministic()
        )
      }.toList }.toList
      
      result(0)._3 must be equalTo false
      result(0)._4 must be equalTo false
      result(1)._3 must be equalTo false
      result(1)._4 must be equalTo false
      result(2)._3 must be equalTo false
      result(2)._4 must be equalTo false
      result(3)._3 must be equalTo false
      result(3)._4 must be equalTo false
      result(4)._3 must be equalTo false
      result(4)._4 must be equalTo false
      result(5)._3 must be equalTo false
      result(5)._4 must be equalTo false
      result(6)._3 must be equalTo false
      result(6)._4 must be equalTo false
    }
    
    "Dump Domain OSM" >> {
      update("""
        CREATE LENS GEO_LENS_DOMAIN_DUMP_OSM 
          AS SELECT * FROM ADDR
        WITH GEOCODE(HOUSE_NUMBER(STRNUMBER),STREET(STRNAME),CITY(CITY),STATE(STATE),GEOCODER(OSM))
      """);
      
      val result = db.query(select("SELECT json_extract(LATITUDE,'$.values') AS DOMAIN_LAT, json_extract(LONGITUDE,'$.values') AS DOMAIN_LON FROM GEO_LENS_DOMAIN_DUMP_OSM"), 
          DumpDomain){ _.map { row => 
        (
          row("DOMAIN_LAT"), 
          row("DOMAIN_LON")
        )
      }.toList }.toList
      
      val jsonLat = play.api.libs.json.Json.parse(result(1)._1.asString)
      val domainLatForRow = jsonLat.asInstanceOf[JsArray].value.map(jsVal => (jsVal.asInstanceOf[JsObject].value("choice").toString(), jsVal.asInstanceOf[JsObject].value("weight").toString().toDouble))
      
      val jsonLon = play.api.libs.json.Json.parse(result(1)._2.asString)
      val domainLonForRow = jsonLon.asInstanceOf[JsArray].value.map(jsVal => (jsVal.asInstanceOf[JsObject].value("choice").toString(), jsVal.asInstanceOf[JsObject].value("weight").toString().toDouble))
      
      domainLatForRow.length must be equalTo domainLonForRow.length
      domainLatForRow must contain(("42.9088720526316",0.321))
      domainLonForRow must contain(("-78.8807727368421", 0.321))
      
    }
    
    "Dump Domain GOOGLE" >> {
      update("""
        CREATE LENS GEO_LENS_DOMAIN_DUMP_GOOGLE 
          AS SELECT * FROM ADDR
        WITH GEOCODE(HOUSE_NUMBER(STRNUMBER),STREET(STRNAME),CITY(CITY),STATE(STATE),GEOCODER(GOOGLE),API_KEY('AIzaSyAKc9sTF-pVezJY8-Dkuvw07v1tdYIKGHk'))
      """);
      
      val result = db.query(select("SELECT json_extract(LATITUDE,'$.values') AS DOMAIN_LAT, json_extract(LONGITUDE,'$.values') AS DOMAIN_LON FROM GEO_LENS_DOMAIN_DUMP_GOOGLE"), 
          DumpDomain){ _.map { row => 
        (
          row("DOMAIN_LAT"), 
          row("DOMAIN_LON")
        )
      }.toList }.toList
      
      val jsonLat = play.api.libs.json.Json.parse(result(1)._1.asString)
      val domainLatForRow = jsonLat.asInstanceOf[JsArray].value.map(jsVal => (jsVal.asInstanceOf[JsObject].value("choice").toString(), jsVal.asInstanceOf[JsObject].value("weight").toString().toDouble))
      
      val jsonLon = play.api.libs.json.Json.parse(result(1)._2.asString)
      val domainLonForRow = jsonLon.asInstanceOf[JsArray].value.map(jsVal => (jsVal.asInstanceOf[JsObject].value("choice").toString(), jsVal.asInstanceOf[JsObject].value("weight").toString().toDouble))
      
      domainLatForRow.length must be equalTo domainLonForRow.length
      domainLatForRow must contain(("42.908687",1.0))
      domainLonForRow must contain(("-78.88065",1.0))
      
    }

  }


}
