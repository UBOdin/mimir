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
    val headerRegex =  """[0-9]*[a-zA-Z_ :\/\\-]+[0-9]*[a-zA-Z0-9_ :\/\\-]*""".r
    header.zipWithIndex.flatMap(el => {
      el._1  match {
        case "NULL" => None
        case headerRegex() => Some(el._2)
        case _ => None
      }
    })
  }
}

@SerialVersionUID(1002L)
class DetectHeaderModel(override val name: String, val targetName:String, val columns:Seq[String], val trainingData:Seq[Seq[PrimitiveValue]])
extends Model(name)
with Serializable
with SourcedFeedback
{
  var headerDetected = false
  var initialHeaders: Map[Int, String] = Map()
  
  private def sanitizeColumnName(name: String): String =
  {
    name.
      replaceAll("[^a-zA-Z0-9]+", "_").    // Replace sequences of non-alphanumeric characters with underscores
      replaceAll("_+$", "").               // Strip trailing underscores
      replaceAll("^_+", "").               // Strip leading underscores
      toUpperCase                          // Capitalize
  }
  
  def detect_header(): (Boolean, Map[Int, String]) = {
    if(trainingData.isEmpty){
      headerDetected = false
      initialHeaders = columns.zipWithIndex.map(el => (el._2 , el._1)).toMap
      (headerDetected,initialHeaders)
    }
    else
    {
      // TODO: COMMENT THIS CODE
      //   From Oliver 12/16/2018: 
      //      This code is really hard to follow.  It needs some serious commenting love.
      val top6 = trainingData
      val (header, topRecords) = (top6.head.map(col => sanitizeColumnName(col match {
          case NullPrimitive() => "NULL" 
          case x => x.asString.toUpperCase()
        })), top6.tail)
      val topRecordsAnalysis = topRecords.foldLeft(Map[Int,Type]())((init, row) => {
        row.zipWithIndex.map(pv => {
           (pv._1 match {
             case NullPrimitive() => TAny()
             case x => {
               BaseType.tests.foldLeft(TAny():Type)((tinit, ttype) => {
                 Cast(ttype._1,x) match {
                   case NullPrimitive() => tinit
                   case x => ttype._1
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
      val conflictOrNullCols = columns.zipWithIndex.unzip._2.toSet -- topRecordsAnalysis.keySet 
      val goodHeaderCols = DetectHeader.isHeader(header) 
      val badHeaderCols = (top6.head.zipWithIndex.unzip._2.toSet -- goodHeaderCols.toSet).toSeq  
      val detectResult = badHeaderCols.flatMap(badCol => {
        top6.head(badCol) match {
          case NullPrimitive() => None
          case StringPrimitive("") => None
          case x => Some(x)
        }
      }) match {
        case Seq() => {
          if(!conflictOrNullCols.isEmpty) DetectHeader.logger.warn(s"There are some type conflicts or nulls in cols: ${conflictOrNullCols.map(columns(_))}") 
          (true, top6.head.zipWithIndex.map(colIdx => (colIdx._2, colIdx._1 match {
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
        case x => (false, header.zipWithIndex.map { x => (x._2, s"COLUMN_${x._2}") }.toMap)
      }
      headerDetected = detectResult._1
      initialHeaders = detectResult._2
      detectResult
    }
  }

  
  def argTypes(idx: Int) = {
    Seq(TInt())
  }
  def varType(idx: Int, args: Seq[BaseType]) = {
    TString()
  }
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ) = {
    getFeedback(idx, args).getOrElse(StringPrimitive(initialHeaders(args(0).asInt)))
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = {
    bestGuess(idx, args, hints)
  }
  def reason(idx: Int, args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): String = {
    getFeedback(idx, args) match {
      case Some(colName) => 
        s"${getReasonWho(idx, args)} told me that $targetName.$colName is a valid column name for column number ${args(0)}"
      case None if headerDetected => 
        s"I analyzed the first several rows of $targetName and there appear to be column headers in the first row.  For column with index: ${args(0)}, the detected header is ${initialHeaders(args(0).asInt)}"
      case None =>
        s"I analyzed the first several rows of $targetName and there do NOT appear to be column headers in the first row.  For the column with index: ${args(0)}, I used the default value of ${initialHeaders(args(0).asInt)}"
    }
  }
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = {
    setFeedback(idx, args, v)
  }
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = {
    hasFeedback(idx, args)
  }
  def hintTypes(idx: Int): Seq[BaseType] = {
    Seq()
  }
  def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue]) = {
    s"${args(0)}"
  }
  def confidence (idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]) : Double = {
    getFeedback(idx, args) match {
      case Some(colName) => 1.0
      case None if headerDetected => 1.0
      case None => 0.5
    }
  }

}
