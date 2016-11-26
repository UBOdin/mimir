package mimir.models

import scala.util._
import mimir.Database
import mimir.algebra.Type.T
import mimir.algebra._
import mimir.ctables.VGTerm
import mimir.optimizer.{InlineVGTerms}
import mimir.util.RandUtils
import org.apache.lucene.search.spell.{
  JaroWinklerDistance, LevensteinDistance, NGramDistance, 
  StringDistance
}

object EditDistanceMatchModel
{
  /**
   * The choice of distance metric, from apache lucene.
   *
   * Options include:
   *  - JaroWinklerDistance()
   *  - LevensteinDistance()
   *  - NGramDistance()
   */
  val defaultMetric: StringDistance = new NGramDistance()

  def train(
    db: Database, 
    source: Either[Operator,List[(String,Type.T)]], 
    target: Either[Operator,List[(String,Type.T)]]
  ): Option[Map[String,(Model,Int)]] = 
  {
    val sourceSch = source match {
        case Left(oper) => oper.schema
        case Right(sch) => sch }
    val targetSch = target match {
        case Left(oper) => oper.schema
        case Right(sch) => sch }

    Some(
      targetSch.map({ case (targetCol,targetType) =>
        targetCol -> (new EditDistanceMatchModel(
          defaultMetric,
          (targetCol, targetType),
          sourceSch.
            filter((x) => isTypeCompatible(targetType, x._2)).
            map( _._1 )
        ), 0)
      }).toMap
    )
  }


  def isTypeCompatible(a: T, b: T): Boolean = 
  {
    (a,b) match {
      case ((Type.TInt|Type.TFloat),  (Type.TInt|Type.TFloat)) => true
      case (Type.TAny, _) => true
      case (_, Type.TAny) => true
      case _ => a == b
    }

  }
}

class EditDistanceMatchModel(
  metric: StringDistance, 
  target: (String, Type.T), 
  sourceCandidates: List[String]
) extends SingleVarModel
{
  var colMapping:List[(String,Double)] = {
    var total = 0.0
    var cumSum = 0.0
    // calculate distance

    sourceCandidates.map( sourceColumn => {
      val dist = metric.getDistance(sourceColumn, target._1)
      total += dist
      (sourceColumn, dist)
    }).
    sortBy(_._2).
    map({ case (k, v) => (k, v / total) })
  } 
  def varType(argTypes: List[Type.T]) = Type.TString

  def sample(randomness: Random, args: List[PrimitiveValue]): PrimitiveValue = 
  {
    StringPrimitive(
      RandUtils.pickFromWeightedList(randomness, colMapping)
    )
  }

  def bestGuess(args: List[PrimitiveValue]): PrimitiveValue = {
    StringPrimitive(  
      colMapping.head._1
    )
  }

  def reason(args: List[Expression]): String = {
    ("I assumed that " + colMapping.head._1 + " maps to " + target._1 + 
      " ("+ (colMapping.head._2 * 100).toInt +"% likely)")
  }
}