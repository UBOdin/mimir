package mimir.models

import scala.util._
import com.typesafe.scalalogging.slf4j.Logger
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
  val logger = Logger(org.slf4j.LoggerFactory.getLogger("mimir.models.EditDistanceMatchModel"))

  /**
   * Available choices of distance metric, from Apache Lucene.
   */
  val metrics = Map[String,StringDistance](
    "NGRAM"       -> new NGramDistance(),
    "LEVENSTEIN"  -> new LevensteinDistance(),
    "JAROWINKLER" -> new JaroWinklerDistance()
  )
  /**
   * Default choice of the distance metric
   */
  val defaultMetric = "NGRAM"

  def train(
    db: Database, 
    name: String,
    source: Either[Operator,List[(String,Type.T)]], 
    target: Either[Operator,List[(String,Type.T)]]
  ): Map[String,(Model,Int)] = 
  {
    val sourceSch = source match {
        case Left(oper) => db.bestGuessSchema(oper)
        case Right(sch) => sch }
    val targetSch = target match {
        case Left(oper) => db.bestGuessSchema(oper)
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

@SerialVersionUID(1000L)
class EditDistanceMatchModel(
  name: String,
  metricName: String,
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
  var colMapping:List[(String,Double)] = 
  {
    val metric = EditDistanceMatchModel.metrics(metricName)
    var cumSum = 0.0
    // calculate distance
    sourceCandidates.map( sourceColumn => {
      val dist = metric.getDistance(sourceColumn, target._1)
      EditDistanceMatchModel.logger.debug(s"Building mapping for $sourceColumn -> $target:  $dist")
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
    val guess = colMapping.maxBy(_._2)._1
    EditDistanceMatchModel.logger.trace(s"Guesssing ($name) $target <- $guess")
    StringPrimitive(guess)
  }

  def reason(args: List[PrimitiveValue]): String = {
    val sourceName = colMapping.maxBy(_._2)._1
    val targetName = target._1
    val editDistance = ((colMapping.head._2) * 100).toInt
    s"I assumed that $sourceName maps to $targetName (Match: $editDistance% using $metricName Distance)"
  }
}