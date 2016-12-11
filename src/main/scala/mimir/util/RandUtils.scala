package mimir.util;

import scala.util.Random;

object RandUtils {

  def pickFromWeightedList[A](rnd: Random, fields: Seq[(A, Double)]): A =
  {
    val tot = fields.map(_._2).fold(0.0)(_+_)
    var pick = rnd.nextFloat() * tot;
    fields.find( (x) => { pick = pick - x._2; pick < 0 }).get._1
  }

  def pickFromList[A](rnd: Random, fields: Seq[A]): A =
  {
    fields(rnd.nextInt(fields.length))
  }
}