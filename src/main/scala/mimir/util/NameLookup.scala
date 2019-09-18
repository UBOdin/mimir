package mimir.util

import sparsity.Name

class NameLookup[T](private val elems: Seq[(Name, T)])
{
  def apply(target: Name):Option[T] = elems.find { _._1.equals(target) }.map { _._2 }
  def keySet = elems.map { _._1 }.toSet
  def toSeq = elems
  def ++(other:NameLookup[T]) = new NameLookup(elems ++ other.elems)
  def values = elems.map { _._2 }
  def keys = elems.map { _._1 }
  def hasKey(target: Name): Boolean = apply(target) != None
  def all: Seq[(Name, T)] = elems
}

object NameLookup 
{
  def apply[T](elems: Iterable[(Name, T)]) = new NameLookup[T](elems.toSeq)
  def apply[T]() = new NameLookup[T](Seq())
  def merge[T](elems: Iterable[NameLookup[T]]) = new NameLookup[T](elems.flatMap{ _.elems }.toSeq)
}