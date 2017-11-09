package mimir.models

import scala.util.Random
import com.typesafe.scalalogging.slf4j.Logger

import scala.collection.mutable.ListBuffer

import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.models._
import mimir.views._
import mimir.util.LoadCSV

object DetectHeader {
  val logger = Logger(org.slf4j.LoggerFactory.getLogger(getClass.getName))
  def isHeader(header:Seq[String]) = {
    val headerRegex =  "[0-9]*[a-zA-Z_ -]+[0-9]*[a-zA-Z_ -]*".r
    header.zipWithIndex.flatMap(el => {
      el._1  match {
        case "NULL" => None
        case headerRegex() => Some(el._2)
        case _ => None
      }
    })
  }
}

@SerialVersionUID(1001L)
class DetectHeaderModel(override val name: String)
extends Model(name)
with Serializable
{
  var headerDetected = false
  
  private def sanitizeColumnName(name: String): String =
  {
    name.
      replaceAll("[^a-zA-Z0-9]+", "_").    // Replace sequences of non-alphanumeric characters with underscores
      replaceAll("_+$", "").               // Strip trailing underscores
      replaceAll("^_+", "").               // Strip leading underscores
      toUpperCase                          // Capitalize
  }
  
  def detect_header(db: Database, query: Operator): (Boolean, Map[Int, String]) = {
    val top6 = db.query(Limit(0,Some(6),query))(_.toList.map(_.tuple)).toSeq
    val (header, topRecords) = (top6.head.map(col => sanitizeColumnName(col match {
        case NullPrimitive() => "NULL" 
        case x => x.asString.toUpperCase()
      })), top6.tail)
    val topRecordsAnalysis = topRecords.foldLeft(Map[Int,Type]())((init, row) => {
      row.zipWithIndex.map(pv => {
         (pv._1 match {
           case NullPrimitive() => TAny()
           case x => {
             Type.rootTypes.foldLeft(TAny():Type)((tinit, ttype) => {
               Cast.apply(ttype,x) match {
                 case NullPrimitive() => tinit
                 case x => ttype
               }
             })
           }
         }) match {
           case TAny() => None
           case x => init.get(pv._2) match {
             case Some(typ) => Some((pv._2 -> x))
             case None => Some((pv._2 -> x))
           }
         }
      }).flatten.toMap
    })
    val dups = collection.mutable.Map( (header.groupBy(identity).collect { case (x, Seq(_,_,_*)) => (x -> 1) }).toSeq: _*)
    val conflictOrNullCols = query.columnNames.zipWithIndex.unzip._2.toSet -- topRecordsAnalysis.keySet 
    val goodHeaderCols = DetectHeader.isHeader(header) 
    val badHeaderCols = (top6.head.zipWithIndex.unzip._2.toSet -- goodHeaderCols.toSet).toSeq  
    badHeaderCols.flatMap(badCol => {
      top6.head(badCol) match {
        case NullPrimitive() => None
        case StringPrimitive("") => None
        case x => Some(x)
      }
    }) match {
      case Seq() => {
        if(!conflictOrNullCols.isEmpty) DetectHeader.logger.warn(s"There are some type conflicts or nulls in cols: ${conflictOrNullCols.map(query.columnNames(_))}") 
        headerDetected = true
        (headerDetected, top6.head.zipWithIndex.map(colIdx => (colIdx._2, colIdx._1 match {
          case NullPrimitive() =>  s"COLUMN_${colIdx._2}"
          case StringPrimitive("") => s"COLUMN_${colIdx._2}"
          case x => {
            val head = sanitizeColumnName(x.asString.toUpperCase())
            dups.get(head) match {
              case Some(dupCnt) => {
                dups(head) = dupCnt+1
                s"${head}_${dupCnt}"
              }
              case None => head
            }
          }
        })).toMap)
      }
      case x => {
        headerDetected = false
      (headerDetected, header.zipWithIndex.map { x => (x._2, s"COLUMN_${x._2}") }.toMap)
      }
    }
  }

  
  def argTypes(idx: Int) = {
    println(s"DetectHeaderModel: argTypes: idx: ${idx}")
      Seq(TRowId())
  }
  def varType(idx: Int, args: Seq[Type]) = {
    println(s"DetectHeaderModel: varType: idx: ${idx}, args: ${args.mkString(",")}")
    TType()
  }
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ) = {
    println(s"DetectHeaderModel: bestGuess: idx: ${idx}, args: ${args.mkString(",")}, hints: ${hints.mkString(",")}")
    hints(0)
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = {
    println(s"DetectHeaderModel: sample: idx: ${idx}, args: ${args.mkString(",")}, hints: ${hints.mkString(",")}")
    hints(0)
  }
  def reason(idx: Int, args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): String = {
    println(s"DetectHeaderModel: reason: idx: ${idx}, args: ${args.mkString(",")}, hints: ${hints.mkString(",")}")
    return ""
  }
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = {
    println(s"DetectHeaderModel: feedback: idx: ${idx}, args: ${args.mkString(",")}, v: ${v.toString}")
    
  }
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = {
    println(s"DetectHeaderModel: isAcknowledged: idx: ${idx}, args: ${args.mkString(",")}")
    return true
  }
  def hintTypes(idx: Int): Seq[mimir.algebra.Type] = {
    println(s"DetectHeaderModel: argTypes: idx: ${idx}")
    Seq(TAny())
  }

}
