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
    name: String, 
    query: Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {
    val operSchema = db.typechecker.schemaOf(query)
    val schemaMap = operSchema.toMap
    
    val houseNumColumn = args.flatMap {
      case Function("HOUSE_NUMBER", cols ) => 
        Some( cols.map { case col:Var => col 
                         case sp@StringPrimitive(manVal) => try {
                             ExpressionParser.expr(manVal) 
                           } catch {
                             case t: Throwable => sp
                           }  
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
      case Function("STREET", cols ) => 
        Some( cols.map { case col:Var => col 
                         case sp@StringPrimitive(manVal) => try {
                             ExpressionParser.expr(manVal) 
                           } catch {
                             case t: Throwable => sp
                           } 
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
      case Function("CITY", cols ) => 
        Some( cols.map { case col:Var => col 
                         case sp@StringPrimitive(manVal) => try {
                             ExpressionParser.expr(manVal) 
                           } catch {
                             case t: Throwable => sp
                           } 
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
      case Function("STATE", cols ) => 
        Some( cols.map { case col:Var => col  
                         case sp@StringPrimitive(manVal) => try {
                             ExpressionParser.expr(manVal) 
                           } catch {
                             case t: Throwable => sp
                           } 
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
      case Function("GEOCODER", cols ) => 
        Some( cols match { case Seq(Var("GOOGLE")) | Seq(StringPrimitive("GOOGLE")) => StringPrimitive("GOOGLE")
                           case Seq(Var("OSM")) | Seq(StringPrimitive("OSM")) => StringPrimitive("OSM")
                           case col => StringPrimitive("OSM")
                       } )
      case _ => None
    }.toSeq match {
      case Seq(x) => x
      case Seq() => StringPrimitive("OSM")
      case x => throw new RAException(s"Invalid geocoder argument: $x in GeocodingLens $name (bad type)")
    }
    
    
    val resultCols = args.flatMap {
      case Function("RESULT_COLUMNS", cols:Seq[Var] @unchecked) => Some( cols.map(_.name) )
      case _ => None
    }.flatten
    
    val projectedOutAddrCols = args.flatMap {
      case Function("HIDE_ADDR_COLUMNS", cols:Seq[Var] @unchecked) => Some( cols.map(_.name) )
      case _ => None
    }.flatten
    
    val resultColNames = resultCols.length match {
      case 0 => Seq("LATITUDE", "LONGITUDE")
      case 1 => Seq(resultCols.head, "LONGITUDE")
      case x => Seq(resultCols(0), resultCols(1))
    }
    
    val inCols = Seq(houseNumColumn, streetColumn, cityColumn, stateColumn, geocoder)
    val (inVars, inExprs) = inCols.map(entry => entry match {
      case v@Var(_) => Seq((Some(v), None) )
      case x => Seq((None, Some(x)))
    }).flatten.unzip
    
    val inColsExprs = s"${inVars.mkString("_")}_${inExprs.mkString.hashCode()}"
    
    val geocodingModel = new GeocodingModel(s"${name}_GEOCODING_MODEL:$inColsExprs", inCols, query) 
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

