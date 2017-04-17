package mimir.exec

import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.algebra._

class SampleResultIterator(
  val src: ResultIterator, 
  val schema: Seq[(String, Type)],
  val nonDet: Set[String],
  val numSamples: Int
)
  extends ResultIterator
  with LazyLogging
{
  val inputs:Seq[Seq[(Int, Double)]] = 
    schema.map { case (name, t) =>
      if(nonDet(name)) {
        (0 until numSamples).map { i => 
          src.schema.indexWhere(_._1.equals(s"MIMIR_SAMPLE_${i}_$name"))
        }.map { colIdx => (colIdx, 1.0 / numSamples) }
      } else {
        Seq( (src.schema.indexWhere(_._1.equals(name)), 1.0) )
      }
    }
  val worldBitsInput = src.schema.indexWhere(_._1.equals("MIMIR_WORLD_BITS"))

  private def values(v: Int): Seq[(PrimitiveValue, Double)] =
    inputs(v).map { case (i, p) => (src(i), p) }

  /**
   * Return the most common value as the "default"
   */
  def apply(v: Int): PrimitiveValue = 
    possibilities(v).toSeq.sortBy(-_._2).head._1

  def open(): Unit = src.open()
  def close(): Unit = src.close()
  def getNext(): Boolean = src.getNext()
  def numCols: Int = schema.size

  def deterministicCol(v: Int): Boolean = (inputs(v).size > 1)
  def deterministicRow(): Boolean = confidence() >= 1.0

  def missingRows(): Boolean = src.missingRows()
  def provenanceToken(): RowIdPrimitive = src.provenanceToken()

  /**
   * Return the probability associated with this row
   */
  def confidence(): Double =
    WorldBits.confidence(src(worldBitsInput).asLong, numSamples)

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