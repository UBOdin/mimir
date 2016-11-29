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
   * The choice of distance metric, from Apache Lucene.
   *
   * Options include:
   *  - JaroWinklerDistance()
   *  - LevensteinDistance()
   *  - NGramDistance()
   */
  val defaultMetric: StringDistance = new NGramDistance()

  def train(
    db: Database, 
    name: String,
    source: Either[Operator,List[(String,Type.T)]], 
    target: Either[Operator,List[(String,Type.T)]]
  ): Map[String,(Model,Int)] = 
  {
    val sourceSch = source match {
        case Left(oper) => oper.schema
        case Right(sch) => sch }
    val targetSch = target match {
        case Left(oper) => oper.schema
        case Right(sch) => sch }

    targetSch.map({ case (targetCol,targetType) =>
      targetCol -> (new EditDistanceMatchModel(
        s"$name:$targetCol",
        defaultMetric,
        (targetCol, targetType),
        sourceSch.
          filter((x) => isTypeCompatible(targetType, x._2)).
          map( _._1 )
      ), 0)
    }).toMap
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
  name: String,
  metric: StringDistance, 
  target: (String, Type.T), 
  sourceCandidates: List[String]
) extends SingleVarModel(name) with Serializable
{
  /** 
   * A mapping for this column.  Lucene Discance metrics use a [0-1] range as:
   *    0 == Strings Are Maximally Different
   *    1 == Strings Are Identical
   * 
   * In other words, distance is a bit of a misnomer.  It's more of a score, which
   * in turn allows us to use it as-is.
   */
  var colMapping:List[(String,Double)] = {
    var cumSum = 0.0
    // calculate distance

    sourceCandidates.map( sourceColumn => {
      val dist = metric.getDistance(sourceColumn, target._1)
      (sourceColumn, dist)
    }).
    map({ case (k, v) => (k, v.toDouble) })
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
      colMapping.maxBy(_._2)._1
    )
  }

  def reason(args: List[Expression]): String = {
    val sourceName = colMapping.head._1
    val targetName = target._1
    val editDistance = (colMapping.head._2) * 100
    s"I assumed that $sourceName maps to $targetName (Match: $editDistance%)"
  }
}