package mimir.algebra.sampling


import play.api.libs.json._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import mimir.algebra.{RAException, Expression}

trait SamplingMode
{
  def apply(plan: LogicalPlan, seed: Long): LogicalPlan
  def expressions: Seq[Expression]
  def rebuildExpressions(x: Seq[Expression]): SamplingMode
  def toJson: JsValue
}

object SamplingMode
{
  val parsers = Seq[Map[String,JsValue] => Option[SamplingMode]](
    SampleStratifiedOn.parseJson,
    SampleRowsUniformly.parseJson
  )

  def fromJson(v: JsValue): SamplingMode =
  {
    val config = v.as[Map[String,JsValue]]
    for(parser <- parsers){
      parser(config) match {
        case Some(s) => return s
        case None => ()
      }
    }
    throw new RAException(s"Invalid Sampling Mode: $v")
  }
}