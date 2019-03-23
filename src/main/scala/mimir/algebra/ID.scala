package mimir.algebra

import sparsity.Name
import scala.language.implicitConversions

/**
 * Generic identifier wrapper for entities in Mimir.  
 * case *sensitive* for now, but decoupling it to make it easier to change 
 * in the future.
 * 
 * - Column / Variable
 * - Table / View / etc...
 * - Mimir Model
 * - Function Names
 * - Aggregate Names
 *
 * In general, we default to using 
 * - UPPERCASE for Columns, Tables, and Models.
 * - lowercase for Functions and Aggregates
 */


case class ID(id: String) 
  extends Serializable
  with ExpressionConstructors
{
  /** 
   * Return a visual representation of the identifier (suitable for debugging)
   */
  override def toString = id
  /**
   * Return the string representation of this identifier (suitable for use in creating new identifiers)
   */
  def asString = id

  // def lower = id
  // def upper = id

  def equals(other: ID) = id.equals(other.id)
  def equals(other: Name) = other.equals(id)
  def withPrefix(prefix: String) = new ID(prefix+id)
  def withSuffix(suffix: String) = new ID(id+suffix)
  def quoted: Name = Name(id, true)
  def equalsIgnoreCase(other: String) = id.equalsIgnoreCase(other)

  def +(other:ID):ID = ID(id+other.id)

  def toExpression:Expression = Var(this)
}

object ID
{
  def apply(prefix: String, id: ID): ID = 
    id.withPrefix(prefix)
  def apply(id: ID, suffix: String): ID = 
    id.withSuffix(suffix)
  def apply(prefix: String, id: ID, suffix: String): ID = 
    id.withPrefix(prefix).withSuffix(suffix)
  def apply(lhs: ID, sep: String, rhs: ID): ID =
    lhs.withSuffix(sep)+rhs
  def apply(elems: Seq[ID], sep: String): ID =
    elems.tail.foldLeft(elems.head)(ID(_, sep, _))
  def upper(id: Name)   = ID(id.upper)
  def lower(id: Name)   = ID(id.lower)
  def upper(id: String) = ID(id.toUpperCase)
  def lower(id: String) = ID(id.toLowerCase)
}

object IDConversions
{
  implicit def StringToID(id: String): ID = ID(id)
}
