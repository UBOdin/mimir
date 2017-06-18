package mimir.ctables

import mimir.algebra._
import mimir.models._
import mimir.util._

sealed trait Repair
{
  def toJSON: String

  def exampleString: String
}

object Repair
{
  def makeRepair(model: Model, idx: Int, v: Seq[PrimitiveValue], h: Seq[PrimitiveValue]): Repair =
  {
    model match {
      case finite:( Model with FiniteDiscreteDomain ) =>
        RepairFromList(finite.getDomain(idx, v, h))
      case _ => 
        RepairByType(model.varType(idx, v.map(_.getType)))
    }
  }
}

case class RepairFromList(choices: Seq[(PrimitiveValue, Double)])
  extends Repair
{
  def toJSON =
    JSONBuilder.dict(Map(
      "selector" -> JSONBuilder.string("list"),
      "values"   -> JSONBuilder.list( choices.map({ case (choice, weight) =>
          JSONBuilder.dict(Map(
            "choice" -> JSONBuilder.string(choice.toString),
            "weight" -> JSONBuilder.double(weight)
          ))
        }))
    ))

  def exampleString =
    s"< pick one of { ${choices.map(_._1).mkString(", ")} } >"
}

case class RepairByType(t: Type)
  extends Repair
{
  def toJSON =
    JSONBuilder.dict(Map(
      "selector" -> JSONBuilder.string("by_type"),
      "type"     -> JSONBuilder.prim(TypePrimitive(t))
    ))

  def exampleString =
  {
    val tString =
      t match {
        case TUser(ut) => ut
        case _ => t.toString
      }
    s"< ${tString} >"
  }
}