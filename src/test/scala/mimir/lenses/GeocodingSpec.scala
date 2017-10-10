package mimir.lenses

import java.io._
import mimir._
import mimir.algebra._
import mimir.test._

import org.specs2.specification._
import org.joda.time.DateTime
import java.util.Locale

object GeocodingSpec 
  extends SQLTestSpecification("GeocodingTest") 
  with BeforeAll
{

  def beforeAll = {
    update("CREATE TABLE ADDR(STRNUMBER varchar, STRNAME varchar, CITY varchar, STATE varchar)")
    update("INSERT INTO ADDR (STRNUMBER, STRNAME, CITY, STATE) VALUES('88', 'Minnessota', 'Buffalo', 'NY' )")
    update("INSERT INTO ADDR (STRNUMBER, STRNAME, CITY, STATE) VALUES('24', 'Custer', 'Buffalo', 'NY' )")
    update("INSERT INTO ADDR (STRNUMBER, STRNAME, CITY, STATE) VALUES('311', 'Bullis', 'West Seneca', 'NY' )")
    update("INSERT INTO ADDR (STRNUMBER, STRNAME, CITY, STATE) VALUES('25', 'Inwood', 'Buffalo', 'NY' )")
    update("INSERT INTO ADDR (STRNUMBER, STRNAME, CITY, STATE) VALUES('74', 'Days', 'Buffalo', 'NY' )")
    update("INSERT INTO ADDR (STRNUMBER, STRNAME, CITY, STATE) VALUES('368', 'Bryant', 'Buffalo', 'NY' )")
    update("INSERT INTO ADDR (STRNUMBER, STRNAME, CITY, STATE) VALUES('10856', 'Wyandale', 'Springville', 'NY' )")
  }
  
  "The Geocoding Lens" should {

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

  }


}
