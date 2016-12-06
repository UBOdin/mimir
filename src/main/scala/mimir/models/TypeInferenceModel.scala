package mimir.models

import scala.util.Random

import mimir.Database
import mimir.algebra._
import mimir.util._

object TypeInferenceModel
{

  val typeTests = List(
    ("(\\+|-)?([0-9]+)",               Type.TInt),
    ("(\\+|-)?([0-9]*(\\.[0-9]+))",    Type.TFloat),
    ("[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}", Type.TDate),
    ("(?i:true|false)",                Type.TBool)
  )

  def detectType(v: String): List[Type.T] = {
    typeTests.flatMap({ case (test, t) =>
      if(v.matches(test)){ Some(t) }
      else { None }
    })
  }
}

class TypeInferenceModel(name: String, column: String, defaultFrac: Double)
  extends SingleVarModel(name)
  with DataIndependentSingleVarFeedback
{
  var totalVotes = 0.0
  val votes = scala.collection.mutable.Map[Type.T, Double]()

  def train(db: Database, query: Operator)
  {
    db.query(
      Project(
        List(ProjectArg(column, Var(column))),
        query
      )
    ).
    mapRows(_(0).asString).
    foreach( v => {
      if(v != null){
        totalVotes += 1.0
        TypeInferenceModel.
          detectType(v).
          foreach( t => { votes(t) += 1.0 } )
      }
    })
  }

  private def voteList = 
    (Type.TString, defaultFrac * totalVotes) :: votes.toList

  def varType(argTypes: List[Type.T]) = Type.TType
  def sample(randomness: Random, args: List[PrimitiveValue]): PrimitiveValue = 
    TypePrimitive(
      RandUtils.pickFromWeightedList(randomness, voteList)
    )

  def bestGuess(args: List[PrimitiveValue]): PrimitiveValue = 
    TypePrimitive(voteList.maxBy(_._2)._1)

  def validateChoice(v: PrimitiveValue): Boolean =
    v.isInstanceOf[TypePrimitive]

  def reason(args: List[Expression]): String = {
    choice match {
      case None => {
        val (guess, guessVotes) = voteList.maxBy(_._2)
        val defaultPct = (defaultFrac * 100).toInt
        val guessPct = ((guessVotes / totalVotes)*100).toInt
        val typeStr = Type.toString(guess)

        val reason =
          guess match {
            case Type.TString =>
              s"not more than $defaultPct% of the data fit anything else"
            case _ => 
              s"around $guessPct% of the data matched"
          }

        s"I guessed that $column was of type $typeStr because $reason"
      }
      case Some(TypePrimitive(t)) =>
        val typeStr = Type.toString(t)
        s"You told me that $column was of type $typeStr"
      case Some(c) =>
        throw new ModelException(s"Invalid choice $c for $name")
    }
  }

}