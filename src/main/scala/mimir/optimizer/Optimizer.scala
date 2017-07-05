package mimir.optimizer

import com.typesafe.scalalogging.slf4j.LazyLogging
import com.github.nscala_time.time.Imports._
import mimir.algebra._
import mimir.util._
import mimir.gprom.algebra.OperatorTranslation

object Optimizer
  extends LazyLogging
{

  /**
   * Optimize the query
   */
  def optimize(rawOper: Operator, opts: Seq[OperatorOptimization]): Operator = {
    var oper = rawOper
    // Repeatedly apply optimizations up to a fixed point or an arbitrary cutoff of 10 iterations
    var startTime = DateTime.now
    TimeUtils.monitor("OPTIMIZE", logger.info(_)){
      for( i <- (0 until 4) ){
        logger.debug(s"Optimizing, cycle $i: \n$oper")
        // Try to optimize
        val newOper = 
          opts.foldLeft(oper) { (o, fn) =>
            logger.trace(s"Applying: $fn")
            fn(o)
          }
          

        // Return if we've reached a fixed point 
        if(oper.equals(newOper)){ return newOper; }

        // Otherwise repeat
        oper = newOper;

        val timeSoFar = startTime to DateTime.now
        if(timeSoFar.millis > 5000){
          logger.warn(s"""OPTIMIZE TIMEOUT (${timeSoFar.millis} ms)
            ---- ORIGINAL QUERY ----\n$rawOper
            ---- CURRENT QUERY (${i+1} iterations) ----\n$oper
            """)
          return oper;
        }
      }
    }
    return oper
  }
  
  def optimize(e:Expression, opts: Seq[ExpressionOptimizerRule]): Expression = {
    opts.foldLeft(e)( (currE, f) => f(currE) )
  }

  def gpromOptimize(rawOper: Operator): Operator = {
    OperatorTranslation.optimizeWithGProM(rawOper)
  }
}