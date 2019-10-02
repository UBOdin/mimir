package mimir.exec.result

import com.typesafe.scalalogging.LazyLogging

import mimir.algebra._
import mimir.exec.mode.{WorldBits,TupleBundle}

class SampleResultIterator(
  val src: ResultIterator, 
  val tupleSchema: Seq[(ID, Type)],
  val nonDet: Set[ID],
  val numSamples: Int
)
  extends ResultIterator
  with LazyLogging
{
  def annotationSchema = src.annotationSchema
  override def schema = tupleSchema.filter(!annotationSchema.contains(_))
  val rowidregex = "MIMIR_ROWID.*".r
  val lookup:Seq[Seq[(Int, Double)]] = 
    schema.map { case (name, t) =>
      if(nonDet(name)) {
        (0 until numSamples).map { i => 
          src.schema.indexWhere(_._1.equals(s"MIMIR_SAMPLE_${i}_$name"))
        }.map { colIdx => (colIdx, 1.0 / numSamples) }
      } else {
        Seq( (src.schema.indexWhere(_._1.equals(name)), 1.0) ) 
      }
    }
  val worldBitsColumnIdx = src.schema.indexWhere(_._1.equals("MIMIR_WORLD_BITS"))


  def close() = src.close()
  def hasNext() = src.hasNext()
  def next() = SampleRow(src.next(), this)
}

case class SampleRow(input: Row, source: SampleResultIterator) extends Row
{
  def annotation(name: ID): PrimitiveValue = input.annotation(name)
  def annotation(idx: Int): PrimitiveValue     = input.annotation(idx)
  def tupleSchema: Seq[(ID, mimir.algebra.Type)] = input.tupleSchema


  private def values(v: Int): Seq[(PrimitiveValue, Double)] =
    source.lookup(v).flatMap {
      case (i, p) => Some((input(i), p))                         
    }

  def tuple: Seq[PrimitiveValue] = 
    (0 until source.lookup.size).map { i => apply(i) }

  /**
   * Return the most common value as the "default"
   */
  def apply(v: Int): PrimitiveValue = 
    possibilities(v).toSeq match {
      //case Seq() => NullPrimitive()
      case x => x.maxBy(_._2)._1
    }

  def deterministicCol(v: Int): Boolean = (source.lookup(v).size > 1)
  def deterministicRow(): Boolean = confidence() >= 1.0

  /**
   * Return the probability associated with this row
   */
  def confidence(): Double =
    WorldBits.confidence(input(source.worldBitsColumnIdx).asLong, source.numSamples)

  /**
   * Return the set of all possible values with their associated probabilities
   */
  def possibilities(v: Int): Map[PrimitiveValue, Double] =
    values(v).groupBy(_._1).mapValues(_.map(_._2).sum)

  /**
   * If this is a numeric column, return the expected value
   */
  def expectation(v: Int): Double =
    values(v).
      map { case (v, p) => v.asDouble * p }.
      sum

  /**
   * If this is a numeric column, return the standard deviation of its possible values
   */
  def stdDev(v: Int): Double =
  {
    val e = expectation(v)
    values(v).
      map { case (v, p) => ((v.asDouble - e) * p) }.
      map { case x => x * x }.
      sum
  }
}