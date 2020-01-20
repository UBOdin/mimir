package mimir.algebra.sampling

import org.apache.spark.sql.functions.{ rand, udf }
import org.apache.spark.sql.catalyst.plans.logical.{ LogicalPlan, Filter }
import play.api.libs.json._
import mimir.algebra._

case class SampleRowsUniformly(probability:Double) extends SamplingMode
{
  override def toString = s"WITH PROBABILITY $probability"

  def apply(plan: LogicalPlan, seed: Long): LogicalPlan = 
  {
    // Adapted from Spark's df.stat.sampleBy method
    val r = rand(seed)
    val f = udf { (x: Double) => x < probability }
    Filter(
      f(r).expr,
      plan
    )
  }
  def expressions: Seq[Expression] = Seq()
  def rebuildExpressions(x: Seq[Expression]): SamplingMode = this


  def toJson: JsValue = JsObject(Map[String,JsValue](
    "mode" -> JsString(SampleRowsUniformly.MODE),
    "probability" -> JsNumber(probability)
  ))
}

object SampleRowsUniformly
{
  val MODE = "uniform_probability"

  def parseJson(json:Map[String, JsValue]): Option[SampleRowsUniformly] =
  {
    println(s"TEST: ${json("mode")} =?= ${MODE}")
    if(json("mode").as[String].equals(MODE)){
      Some(SampleRowsUniformly(json("probability").as[Double]))
    } else {
      None
    }
  }
}