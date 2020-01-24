package mimir.algebra.sampling

import org.apache.spark.sql.functions.{rand, udf, col}
import org.apache.spark.sql.catalyst.plans.logical.{ LogicalPlan, Filter }
import play.api.libs.json._
import mimir.algebra._
import mimir.exec.spark.RAToSpark
import mimir.serialization.AlgebraJson._

/** 
 * Generate a sample of the dataset stratified on the specified column
 * 
 * A stratified sample allows sampling with variable rates depending on 
 * the value of the specified column.  Its most frequent use is to ensure
 * fairness between strata, regardless of their distribution in the 
 * original dataset.  For example, this could be used to derive a sample
 * of demographic data with equal representations from all ethnicities, 
 * even if one ethnicity is under-represented.
 * 
 * Sampling rate is given as a probability.  The final sample will 
 * contain approximately `strata(value) * count(df.col = value)` records
 * where `df.col = value`.  
 *
 * @param    column    The column to use to determine the sampling rate
 * @param    strata    A map from a value for [[column]] to the probability of 
 *                     sampling the value. Non-specified values will not be 
 *                     included in the sample.
 **/
case class SampleStratifiedOn(column:ID, t:Type, strata:Map[PrimitiveValue,Double]) extends SamplingMode
{
  val sparkStrata = 
    strata.map { case (v, p) => RAToSpark.getNative(v, t) -> p }
          .toMap

  override def toString = s"ON $column WITH STRATA ${strata.map { case (v,p) => s"$v -> $p"}.mkString(" | ")}"


  def apply(plan: LogicalPlan, seed: Long): LogicalPlan =
  {
    // Adapted from Spark's df.stat.sampleBy method
    val c = col(column.id)
    val r = rand(seed)
    val f = udf { (stratum: Any, x: Double) =>
              x < sparkStrata.getOrElse(stratum, 0.0)
            }
    Filter(
      f(c, r).expr,
      plan
    )
  }
  def expressions: Seq[Expression] = Seq(Var(column))
  def rebuildExpressions(x: Seq[Expression]): SamplingMode =
  {
    x(0) match { 
      case Var(newColumn) => SampleStratifiedOn(newColumn, t, strata) 
      case _ => throw new RAException("Internal Error: Rewriting stratification variable with arbitrary expression")
    }
  }

  def toJson: JsValue = JsObject(Map[String,JsValue](
    "mode" -> JsString(SampleStratifiedOn.MODE),
    "column" -> JsString(column.id),
    "type" -> Json.toJson(t),
    "strata" -> JsArray(
      strata
        .toSeq
        .map { case (v, p) => JsObject(Map[String,JsValue](
            "value" -> Json.toJson(v),
            "probability" -> JsNumber(p)
          ))
        }
    )
  ))
}

object SampleStratifiedOn
{
  val MODE = "stratified_on"

  def parseJson(json:Map[String, JsValue]): Option[SampleStratifiedOn] =
  {
    if(json("mode").as[String].equals(MODE)){
      val t = json("type").as[Type]

      Some(SampleStratifiedOn(
        ID(json("column").as[String]),
        t,
        json("strata")
          .as[Seq[Map[String,JsValue]]]
          .map { stratum => 
            castJsonToPrimitive(t, stratum("value")) -> 
              stratum("probability").as[Double]
          }
          .toMap
      ))
    } else {
      None
    }
  }
}