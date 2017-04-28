package mimir.test

trait TestTimer
{
  def getTime: Double = System.nanoTime() / 1000.0 / 1000.0 / 1000.0

  def time[F](anonFunc: => F): (F, Double) = {
      val tStart = getTime
      val anonFuncRet = anonFunc
      val tEnd = getTime
      (anonFuncRet, (tEnd-tStart).toDouble)
    }
}