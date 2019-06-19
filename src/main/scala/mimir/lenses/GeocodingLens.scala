package mimir.lenses

import java.io.File
import java.sql._
import java.util

import mimir.algebra._
import mimir.ctables._
import mimir.util.RandUtils
import mimir.Database
import mimir.parser._
import mimir.models._

import scala.collection.JavaConversions._
import scala.util._

object GeocodingLens {

  def create(
    db: Database, 
    name: ID, 
    humanReadableName: String,
    query: Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {
    val operSchema = db.typechecker.schemaOf(query)
    val schemaMap = operSchema.toMap
    
    val houseNumColumn = args.flatMap {
      case Function(ID("house_number"), cols ) => 
        Some( cols.map { case col:Var => col 
                         case manVal:StringPrimitive => manVal 
                         case col => throw new RAException(s"Invalid House Number col argument: $col in GeocodingLens $name (not a column reference)")
                       } )
      case _ => None
    }.toSeq.flatten match {
      case Seq(vcol@Var(col)) => vcol
      case Seq(sval@StringPrimitive(x)) => sval
      case Seq() => StringPrimitive("")
      case x => throw new RAException(s"Invalid house number argument: $x in GeocodingLens $name (bad type)")
    }
    
    val streetColumn = args.flatMap {
      case Function(ID("street"), cols ) => 
        Some( cols.map { case col:Var => col 
                         case manVal:StringPrimitive => manVal 
                         case col => throw new RAException(s"Invalid street argument: $col in GeocodingLens $name (not a column reference or string value)")
                       } )
      case _ => None
    }.toSeq.flatten match {
      case Seq(vcol@Var(col)) => vcol
      case Seq(sval@StringPrimitive(x)) => sval
      case Seq() => StringPrimitive("")
      case x => throw new RAException(s"Invalid street argument: $x in GeocodingLens $name (bad type)")
    }
    
    val cityColumn = args.flatMap {
      case Function(ID("city"), cols ) => 
        Some( cols.map { case col:Var => col 
                         case manVal:StringPrimitive => manVal  
                         case col => throw new RAException(s"Invalid City Col argument: $col in GeocodingLens $name (not a column reference)")
                       } )
      case _ => None
    }.toSeq.flatten match {
      case Seq(vcol@Var(col)) => vcol
      case Seq(sval@StringPrimitive(x)) => sval
      case Seq() => StringPrimitive("")
      case x => throw new RAException(s"Invalid city argument: $x in GeocodingLens $name (bad type)")
    }
    
    val stateColumn = args.flatMap {
      case Function(ID("state"), cols ) => 
        Some( cols.map { case col:Var => col  
                         case manVal:StringPrimitive => manVal 
                         case col => throw new RAException(s"Invalid State Col argument: $col in GeocodingLens $name (not a column reference)")
                       } )
      case _ => None
    }.toSeq.flatten match {
      case Seq(vcol@Var(col)) => vcol
      case Seq(sval@StringPrimitive(x)) => sval
      case Seq() => StringPrimitive("")
      case x => throw new RAException(s"Invalid state argument: $x in GeocodingLens $name (bad type)")
    }
    
    val geocoder = args.flatMap {
      case Function(ID("geocoder"), cols ) => 
        Some( cols match { case Seq(Var(ID("GOOGLE"))) | Seq(StringPrimitive("GOOGLE")) => StringPrimitive("GOOGLE")
                           case Seq(Var(ID("OSM"))) | Seq(StringPrimitive("OSM")) => StringPrimitive("OSM")
                           case col => StringPrimitive("OSM")
                       } )
      case _ => None
    }.toSeq match {
      case Seq(x) => x
      case Seq() => StringPrimitive("OSM")
      case x => throw new RAException(s"Invalid geocoder argument: $x in GeocodingLens $name (bad type)")
    }
    
    val defaultAPIKey = "AIzaSyAKc9sTF-pVezJY8-Dkuvw07v1tdYIKGHk"
    val apiKey = args.flatMap {
      case Function(ID("api_key"), cols ) => cols match { 
        case Seq(StringPrimitive(key)) => Some(key)
        case x => None
      } 
      case _ => None
    }.toSeq match {
      case Seq(x) => x
      case Seq() => defaultAPIKey
      case x => throw new RAException(s"Invalid api key argument: $x in GeocodingLens $name (bad type)")
    }
    
    val resultCols = args.flatMap {
      case Function(ID("result_columns"), cols:Seq[Var] @unchecked) => Some( cols.map(_.name) )
      case _ => None
    }.flatten
    
    val projectedOutAddrCols = args.flatMap {
      case Function(ID("hide_addr_columns"), cols:Seq[Var] @unchecked) => Some( cols.map(_.name) )
      case _ => None
    }.flatten
    
    val resultColNames = resultCols.length match {
      case 0 => Seq(ID("LATITUDE"), ID("LONGITUDE"))
      case 1 => Seq(resultCols.head, ID("LONGITUDE"))
      case x => Seq(resultCols(0), resultCols(1))
    }
    
    val inCols = Seq(houseNumColumn, streetColumn, cityColumn, stateColumn)
    val inColsStr = inCols.map(entry => entry match {
      case StringPrimitive(x) => x
      case Var(x) => x
      case x => ???
    }).mkString("_")
    
    
    
    val geocodingModel = new GeocodingModel(ID(name,"_GEOCODING_MODEL:"+inColsStr), inCols, ID(geocoder.asString), apiKey, query) 
    geocodingModel.reconnectToDatabase(db)
    
    val projectArgs = 
      query.columnNames.
        flatMap( col => 
          if(projectedOutAddrCols.contains(col))
            None
          else
            Some(ProjectArg(col, Var(col)))
        ).union(Seq( 
            ProjectArg(resultColNames(0), VGTerm(geocodingModel.name, 0,Seq[Expression](RowIdVar())++inCols, Seq()) ), 
            ProjectArg(resultColNames(1), VGTerm(geocodingModel.name, 1,Seq[Expression](RowIdVar())++inCols, Seq()) ) 
        ))

    return (
      Project(projectArgs, query),
      Seq(geocodingModel)
    )
  }
}

