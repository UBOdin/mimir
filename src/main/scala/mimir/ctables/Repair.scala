package mimir.ctables

import java.sql.SQLException

import mimir.Database
import mimir.algebra._
import mimir.util._

sealed trait Repair
{
  def toJSON: String

  def exampleString: String

  def apply(db: Database, fix: Seq[Expression])
}

object Repair
{
  // def makeRepair(model: Model, idx: Int, v: Seq[PrimitiveValue], h: Seq[PrimitiveValue]): Repair =
  // {
  //   model match {
  //     case finite:( Model with FiniteDiscreteDomain ) =>
  //       RepairFromList(finite.getDomain(idx, v, h))
  //     case _ => 
  //       RepairByType(model.varType(idx, v.map(_.getType)))
  //   }
  // }
}

// case class RepairFromList(choices: Seq[(PrimitiveValue, Double)])
//   extends Repair
// {
//   def toJSON =
//     JSONBuilder.dict(Map(
//       "selector" -> "list",
//       "values"   -> choices.map({ case (choice, weight) =>
//           Map(
//             "choice" -> choice.toString,
//             "weight" -> weight
//           )
//         })
//     ))

//   def exampleString =
//     s"< pick one of { ${choices.map(_._1).mkString(", ")} } >"
// }

// case class RepairByType(t: Type)
//   extends Repair
// {
//   def toJSON =
//     JSONBuilder.dict(Map(
//       "selector" -> "by_type",
//       "type"     -> TypePrimitive(t)
//     ))

//   def exampleString =
//   {
//     val tString =
//       t match {
//         case TUser(ut) => ut
//         case TString() => "any string"
//         case _ => t.toString
//       }
//     s"<${tString}>"
//   }
// }

// case class ModerationRepair(userRepairedValue:String) 
//   extends Repair 
// {
//   def toJSON: String = JSONBuilder.dict(Map( "value" -> userRepairedValue))
//   def exampleString: String = s"< $userRepairedValue >"
// }

object SimpleCaveatRepair
  extends Repair
{
  def toJSON =
    JSONBuilder.dict(Map(
      "selector" -> JSONBuilder.string("caveat")
    ))
  def exampleString: String = "< Explain why in a string >"
  def apply(db: Database, fix: Seq[Expression]) = 
    throw new SQLException("This caveat can not be repaired, only acknowledged")
}
object EmptyReasonSetRepair
  extends Repair
{
  def toJSON =
    JSONBuilder.dict(Map(
      "selector" -> JSONBuilder.string("empty_reasons")
    ))
  def exampleString: String = "< Explain why in a string >"
  def apply(db: Database, fix: Seq[Expression]) = 
    throw new SQLException("Turns out there aren't any instances of this caveat affecting the result")
}