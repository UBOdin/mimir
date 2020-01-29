package mimir.algebra

import sparsity.Name
import scala.language.implicitConversions
import play.api.libs.json._

/**
 * Generic identifier wrapper for entities in Mimir.  
 * 
 *************  WARNING WARNING WARNING WARNING **************
 ****                                                     ****
 *  READ THIS TEXT COMPLETELY BEFORE DONG ANYTHING WITH IDs  *
 ****                                                     ****
 *************  WARNING WARNING WARNING WARNING **************
 * 
 ********** BEGIN Message from supreme high leader Oliver ***********
 * I don't want to see *anywhere* in the code ANY of the following
 * - [var].id.toUpperCase
 * - [var].id.toLowerCase
 * - [var].id.equalsIgnoreCase
 * or anything along these lines.  In fact, unless you have a 
 * particularly good reason to do so (several acceptable reasons
 * listed below), you should NEVER access [var].id.  Acceptable
 * reasons include:
 * - You're talking to a backend that is case sensitive (e.g. Spark)
 * - You're printing debug information.
 * If you're talking to a mixed-case backend (e.g., GProM), ID
 * values MUST be treated as quoted.
 ********** END Message from supreme high leader Oliver ***********
 * 
 * In short, ID is an identifier THAT HAS ALREADY BEEN
 * RESOLVED INTO CASE SENSITIVE FORM.  Broadly, this should be
 * done with one of the following two methods:
 * - ID.upper(v)
 * - ID.lower(v)
 * Both of these methods work on both String and sparsity.Name
 * and have the correct behavior for Sparsity Name quoting.
 * Case-insensitive name resolution methods appear in the following
 * places:
 * - Tables: Database.resolveCaseInsensitiveTable
 * - Columns: Operator.resolveCaseInsensitiveColumn
 * 
 * Use ID.upper for the following:
 * - Column/Variable names
 * - Table/View names
 * - Mimir Model names
 * Use ID.lower for the following:
 * - Function names
 * - Aggregate names
 * 
 * To convert back to SQL, use [var].quoted.
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
  implicit val format: Format[ID] = Format(
    JsPath.read[String].map { ID(_) },
    new Writes[ID]{ def writes(i:ID) = JsString(i.id) }
  )
}

object IDConversions
{
  implicit def StringToID(id: String): ID = ID(id)
}
