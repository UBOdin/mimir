package mimir.algebra.spark.function

import mimir.algebra.function.FunctionRegistry
import mimir.algebra.PrimitiveValue
import mimir.algebra.BaseType
import com.typesafe.scalalogging.slf4j.LazyLogging


object SparkFunctions 
 extends LazyLogging {
  
  val sparkFunctions = scala.collection.mutable.Map[String, (Seq[PrimitiveValue] => PrimitiveValue, Seq[BaseType] => BaseType)]()
  
  def addSparkFunction(fname:String,eval:Seq[PrimitiveValue] => PrimitiveValue, typechecker: Seq[BaseType] => BaseType) : Unit = {
    sparkFunctions.put(fname, (eval, typechecker))
  }
  
  def register(fr: FunctionRegistry)
  {
    sparkFunctions.foreach(sfunc => {
      logger.debug("registering spark func: " + sfunc._1)
      fr.registerPassthrough(sfunc._1, sfunc._2._1, sfunc._2._2)
    })
  }
  
  
}
