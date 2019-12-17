package mimir.lenses

import java.io._
import mimir._
import mimir.algebra._
import mimir.test._

import org.specs2.specification._
import org.joda.time.DateTime
import java.util.Locale
import play.api.libs.json. { JsArray, JsValue, JsObject }

import sparsity.Name
import sparsity.statement.CreateView
import mimir.parser._

object GeocodingSpec 
  extends SQLTestSpecification("GeocodingTest") 
  with BeforeAll
{

  def beforeAll = {
    loadCSV(
      targetTable = "ADDR",
      sourceFile = "test/data/geo.csv", 
      inferTypes = false, 
      detectHeaders =true
    )
  }
  
  "The Geocoding Lens" should {
    sequential 
    
    //spark cluster mode failures
    /*"Be able to create and query geocoding lenses with google geocoder" >> {
 
      val viewQuery0 = sparsity.parser.SQL("SELECT  ROWID() AS RID, ADDR.* FROM ADDR;") match {
            case fastparse.Parsed.Success(sparsity.statement.Select(body, _), _) => body
            case x => throw new Exception(s"Invalid view query : \n $x")
          }
         db.update(SQLStatement(CreateView(Name("ADDR_VIEW", true), false, viewQuery0)))
      
      update("""
        CREATE LENS GEO_LENS_GOOGLE 
          AS SELECT * FROM ADDR_VIEW
        WITH GEOCODE(HOUSE_NUMBER(STRNUMBER),STREET(STRNAME),CITY(CITY),STATE(STATE),GEOCODER(GOOGLE))
      """);

      val result = query("""
        SELECT LATITUDE, LONGITUDE FROM GEO_LENS_GOOGLE
      """)(results => results.toList.map( row =>  { 
        (
          row(ID("LATITUDE")), 
          row(ID("LONGITUDE")), 
          row.isColDeterministic(ID("LATITUDE")),
          row.isColDeterministic(ID("LONGITUDE")),
          row.isDeterministic()
        )
       }))
       
       val viewQuery = sparsity.parser.SQL("SELECT ROWID() AS RID,  LATITUDE AS LATITUDE, LONGITUDE AS LONGITUDE FROM GEO_LENS_GOOGLE;") match {
            case fastparse.Parsed.Success(sparsity.statement.Select(body, _), _) => body
            case x => throw new Exception(s"Invalid view query : \n $x")
          }
         db.update(SQLStatement(CreateView(Name("GEO_LENS_GOOGLE_VIEW", true), false, viewQuery)))
      
      val viewQuery1 = sparsity.parser.SQL("SELECT  RID AS RID,  LATITUDE AS LATITUDE, LONGITUDE AS LONGITUDE FROM GEO_LENS_GOOGLE_VIEW WHERE LATITUDE > 40.0 AND LATITUDE < 50.0;") match {
            case fastparse.Parsed.Success(sparsity.statement.Select(body, _), _) => body
            case x => throw new Exception(s"Invalid view query : \n $x")
          }
         db.update(SQLStatement(CreateView(Name("GEO_LENS_GOOGLE_VIEW_VIEW", true), false, viewQuery1)))
         
      val result2 = query("""
        SELECT  RID AS RID,  LATITUDE AS LATITUDE, LONGITUDE AS LONGITUDE FROM GEO_LENS_GOOGLE_VIEW_VIEW
      """)(results => results.toList.map( row =>  { 
        (
          row(ID("LATITUDE")), 
          row(ID("LONGITUDE")), 
          row.isColDeterministic(ID("LATITUDE")),
          row.isColDeterministic(ID("LONGITUDE")),
          row.isDeterministic()
        )
       }))
         
      result(0)._1 must not be equalTo(NullPrimitive())
      result(0)._2 must not be equalTo(NullPrimitive())
      result(0)._3 must be equalTo false
      result(0)._4 must be equalTo false
      result(1)._1 must not be equalTo(NullPrimitive())
      result(1)._2 must not be equalTo(NullPrimitive())
      result(1)._3 must be equalTo false
      result(1)._4 must be equalTo false
      result(2)._1 must not be equalTo(NullPrimitive())
      result(2)._2 must not be equalTo(NullPrimitive())
      result(2)._3 must be equalTo false
      result(2)._4 must be equalTo false
      result(3)._1 must not be equalTo(NullPrimitive())
      result(3)._2 must not be equalTo(NullPrimitive())
      result(3)._3 must be equalTo false
      result(3)._4 must be equalTo false
      result(4)._1 must not be equalTo(NullPrimitive())
      result(4)._2 must not be equalTo(NullPrimitive())
      result(4)._3 must be equalTo false
      result(4)._4 must be equalTo false
      result(5)._1 must not be equalTo(NullPrimitive())
      result(5)._2 must not be equalTo(NullPrimitive())
      result(5)._3 must be equalTo false
      result(5)._4 must be equalTo false
      result(6)._1 must not be equalTo(NullPrimitive())
      result(6)._2 must not be equalTo(NullPrimitive())
      result(6)._3 must be equalTo false
      result(6)._4 must be equalTo false
      
      result2(0)._1 must not be equalTo(NullPrimitive())
      result2(0)._2 must not be equalTo(NullPrimitive())
      result2(0)._3 must be equalTo false
      result2(0)._4 must be equalTo false
      result2(1)._1 must not be equalTo(NullPrimitive())
      result2(1)._2 must not be equalTo(NullPrimitive())
      result2(1)._3 must be equalTo false
      result2(1)._4 must be equalTo false
      result2(2)._1 must not be equalTo(NullPrimitive())
      result2(2)._2 must not be equalTo(NullPrimitive())
      result2(2)._3 must be equalTo false
      result2(2)._4 must be equalTo false
      result2(3)._1 must not be equalTo(NullPrimitive())
      result2(3)._2 must not be equalTo(NullPrimitive())
      result2(3)._3 must be equalTo false
      result2(3)._4 must be equalTo false
      result2(4)._1 must not be equalTo(NullPrimitive())
      result2(4)._2 must not be equalTo(NullPrimitive())
      result2(4)._3 must be equalTo false
      result2(4)._4 must be equalTo false
      result2(5)._1 must not be equalTo(NullPrimitive())
      result2(5)._2 must not be equalTo(NullPrimitive())
      result2(5)._3 must be equalTo false
      result2(5)._4 must be equalTo false
      result2(6)._1 must not be equalTo(NullPrimitive())
      result2(6)._2 must not be equalTo(NullPrimitive())
      result2(6)._3 must be equalTo false
      result2(6)._4 must be equalTo false
      
    }*/
    
    "Be able to sucessfully make web requests" >> {
      
      val result = query("""
        SELECT WEBJSON('http://api.geonames.org/timezoneJSON?lat=42.80&lng=-78.89&username=ubodintestcase', '.time') AS NOW_TIME FROM ADDR
      """){ _.map { row => 
        
         row(ID("NOW_TIME")).asString
        
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
          row(ID("LATITUDE")), 
          row(ID("LONGITUDE")), 
          row.isColDeterministic(ID("LATITUDE")),
          row.isColDeterministic(ID("LONGITUDE")),
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
          row(ID("LATITUDE")), 
          row(ID("LONGITUDE")), 
          row.isColDeterministic(ID("LATITUDE")),
          row.isColDeterministic(ID("LONGITUDE")),
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
      
      val result = 
        db.query(select("""
          SELECT json_extract(LATITUDE,'$.values') AS DOMAIN_LAT, 
                 json_extract(LONGITUDE,'$.values') AS DOMAIN_LON 
          FROM GEO_LENS_DOMAIN_DUMP_OSM"""
        )){ _.map { row => 
            (
              row(ID("DOMAIN_LAT")), 
              row(ID("DOMAIN_LON"))
            )
          }.toIndexedSeq
        }
      
      result(1)._1.asDouble must beCloseTo(42.9088720526316 within 2.significantFigures)
      result(1)._2.asDouble must beCloseTo(-78.8807727368421 within 2.significantFigures)
      
    }
    
    "Dump Domain GOOGLE" >> {
      update("""
        CREATE LENS GEO_LENS_DOMAIN_DUMP_GOOGLE 
          AS SELECT * FROM ADDR
        WITH GEOCODE(HOUSE_NUMBER(STRNUMBER),STREET(STRNAME),CITY(CITY),STATE(STATE),GEOCODER(GOOGLE),API_KEY('AIzaSyAKc9sTF-pVezJY8-Dkuvw07v1tdYIKGHk'))
      """);
      
      val result = db.query(select("""
        SELECT json_extract(LATITUDE,'$.values') AS DOMAIN_LAT, 
               json_extract(LONGITUDE,'$.values') AS DOMAIN_LON 
        FROM GEO_LENS_DOMAIN_DUMP_GOOGLE"""
      )){ _.map { row => 
        (
          row(ID("DOMAIN_LAT")), 
          row(ID("DOMAIN_LON"))
        )
      }.toList }.toList
            
      result(1)._1.asDouble must beCloseTo(42.908687 within 2.significantFigures)
      result(1)._2.asDouble must beCloseTo(-78.88065 within 2.significantFigures)

    }

  }


}
