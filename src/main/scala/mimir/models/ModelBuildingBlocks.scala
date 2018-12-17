package mimir.models

import mimir.algebra._

trait NoArgModel
{
  def argTypes(x: Int): Seq[BaseType] = List()
  def hintTypes(idx: Int) = Seq()
}

trait ModelCache
{
  val cache = scala.collection.mutable.Map[String,PrimitiveValue]()
  def getCacheKey(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue] ) : String
  def getCache(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) : Option[PrimitiveValue] = {
    cache.get(getCacheKey(idx,args,hints))
  }
  def setCache(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue], value:PrimitiveValue) : Unit = {
    cache(getCacheKey(idx,args,hints)) = value 
  }
}