package mimir.models

import scala.util._
import com.typesafe.scalalogging.slf4j.Logger
import mimir.Database
import mimir.algebra._
import mimir.util.RandUtils
import org.apache.lucene.search.spell.{
  JaroWinklerDistance, LevensteinDistance, NGramDistance,
  StringDistance
}

object EditDistanceMatchModel
{
  val logger = Logger(org.slf4j.LoggerFactory.getLogger(getClass.getName))

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
    source: Either[Operator,Seq[(String,Type)]],
    target: Either[Operator,Seq[(String,Type)]]
  ): Map[String,(Model,Int)] =
  {
    val sourceSch: Seq[(String,Type)] = source match {
        case Left(oper) => db.bestGuessSchema(oper)
        case Right(sch) => sch }
    val targetSch: Seq[(String,Type)] = target match {
        case Left(oper) => db.bestGuessSchema(oper)
        case Right(sch) => sch }

    targetSch.map({ case (targetCol,targetType) =>
      ( targetCol,
        (new EditDistanceMatchModel(
          s"$name:$targetCol",
          defaultMetric,
          (targetCol, targetType),
          sourceSch.
            filter((x) => isTypeCompatible(targetType, x._2)).
            map( _._1 )
        ), 0))
    }).toMap
  }

  def isTypeCompatible(a: Type, b: Type): Boolean =
  {
    val aBase = Type.rootType(a)
    val bBase = Type.rootType(b)
    (aBase, bBase) match {
      case ((TInt()|TFloat()),  (TInt()|TFloat())) => true
      case (TAny(), _) => true
      case (_, TAny()) => true
      case _ => aBase == bBase
    }

  }
}

@SerialVersionUID(1001L)
class EditDistanceMatchModel(
  name: String,
  metricName: String,
  target: (String, Type),
  sourceCandidates: Seq[String]
)
  extends Model(name)
  with DataIndependentFeedback
  with NoArgModel
  with FiniteDiscreteDomain
{
  /**
   * A mapping for this column.  Lucene Discance metrics use a [0-1] range as:
   *    0 == Strings Are Maximally Different
   *    1 == Strings Are Identical
   *
   * In other words, distance is a bit of a misnomer.  It's more of a score, which
   * in turn allows us to use it as-is.
   */
  var colMapping:IndexedSeq[(String,Double)] =
  {
    val metric = EditDistanceMatchModel.metrics(metricName)
    var cumSum = 0.0
    // calculate distance
    sourceCandidates.map( sourceColumn => {
      val dist = metric.getDistance(sourceColumn, target._1)
      EditDistanceMatchModel.logger.debug(s"Building mapping for $sourceColumn -> $target:  $dist")
      (sourceColumn, dist)
    }).
    map({ case (k, v) => (k, v.toDouble) }).
    toIndexedSeq
  }
  def varType(idx: Int, t: Seq[Type]) = TString()

  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue =
  {
    StringPrimitive(
      RandUtils.pickFromWeightedList(randomness, colMapping)
    )
  }

  def validateChoice(idx: Int, v: PrimitiveValue): Boolean =
  {
    EditDistanceMatchModel.logger.debug(s"Validate Edit Distance $name -> $v")
    sourceCandidates.contains(v.asString)
  }

  def reason(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): String = {
    choices(idx) match {
      case None => {
        val sourceName = colMapping.maxBy(_._2)._1
        val targetName = target._1
        val editDistance = ((colMapping.head._2) * 100).toInt
        s"I assumed that $sourceName maps to $targetName (Match: $editDistance% using $metricName Distance)"
      }

      case Some(NullPrimitive()) => {
        val targetName = target._1
        s"${getReasonWho(idx,args)} told me that nothing maps to $targetName"
      }

      case Some(choicePrim) => {
        val targetName = target._1
        val choiceStr = choicePrim.asString
        s"${getReasonWho(idx,args)} told me that $choiceStr maps to $targetName"
      }
    }
  }

  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue =
  {
    choices(idx) match {
      case None => {
        if(colMapping.isEmpty){
          NullPrimitive()
        } else {
          val guess = colMapping.maxBy(_._2)._1
          EditDistanceMatchModel.logger.trace(s"Guesssing ($name) $target <- $guess")
          StringPrimitive(guess)
        }
      }
      case Some(s) => s
    }
  }

  def getDomain(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): Seq[(PrimitiveValue,Double)] =
    List((NullPrimitive(), 0.0)) ++ colMapping.map( x => (StringPrimitive(x._1), x._2))

  def confidence (idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]): Double  = {
    choices(idx) match {
      case None => {
        if(colMapping.isEmpty){
          0.0
        } else {
          colMapping.maxBy(_._2)._2/colMapping.map(_._2).sum
        }
      }
      case Some(s) => 1.0
    }
  }
}
