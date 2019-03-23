package mimir.models

import mimir.algebra._

trait NoArgModel
{
  def argTypes(x: Int): Seq[Type] = List()
  def hintTypes(idx: Int) = Seq()
}

trait ModelCache
{
  val cache = scala.collection.mutable.Map[ID,PrimitiveValue]()
  def getCacheKey(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue] ) : ID
  def getCache(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) : Option[PrimitiveValue] = {
    cache.get(getCacheKey(idx,args,hints))
  }
  def setCache(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue], value:PrimitiveValue) : Unit = {
    cache(getCacheKey(idx,args,hints)) = value 
  }
}