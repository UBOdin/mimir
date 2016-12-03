package mimir.models

import scala.util.Random
import com.typesafe.scalalogging.slf4j.Logger

import mimir.Database
import mimir.algebra._
import mimir.util._

object TypeInferenceModel
{
  val logger = Logger(org.slf4j.LoggerFactory.getLogger("mimir.models.TypeInferenceModel"))

  val typeTests = List(
    ("(\\+|-)?([0-9]+)",               Type.TInt),
    ("(\\+|-)?([0-9]*(\\.[0-9]+)?)",   Type.TFloat),
    ("[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}", Type.TDate),
    ("(?i:true|false)",                Type.TBool)
  )

  val priority = Map(
    Type.TInt    -> 10,
    Type.TBool   -> 10,
    Type.TDate   -> 10,
    Type.TFloat  -> 5,
    Type.TString -> 0
  )

  def detectType(v: String): List[Type.T] = {
    typeTests.flatMap({ case (test, t) =>
      if(v.matches(test)){ Some(t) }
      else { None }
    })
  }
}

@SerialVersionUID(1000L)
class TypeInferenceModel(name: String, column: String, defaultFrac: Double)
  extends SingleVarModel(name)
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
    mapRows(_(0)).
    filter({ 
      case null            => false
      case NullPrimitive() => false
      case _               => true
    }).
    map(_.asString).
    foreach( learn(_) )
  }

  def learn(v: String)
  {
    totalVotes += 1.0
    val candidates = TypeInferenceModel.detectType(v)
    TypeInferenceModel.logger.debug(s"Guesses for '$v': $candidates")
    candidates.foreach( t => { votes(t) = votes.getOrElse(t, 0.0) + 1.0 } )
  }

  private final def voteList = 
    (Type.TString, defaultFrac * totalVotes) :: votes.toList

  private final def rankFn(x:(Type.T, Double)) =
    (x._2, TypeInferenceModel.priority(x._1) )

  def varType(argTypes: List[Type.T]) = Type.TType
  def sample(randomness: Random, args: List[PrimitiveValue]): PrimitiveValue = 
    TypePrimitive(
      RandUtils.pickFromWeightedList(randomness, voteList)
    )

  def bestGuess(args: List[PrimitiveValue]): PrimitiveValue = 
  {
    val guess = voteList.maxBy( rankFn _ )._1
    TypeInferenceModel.logger.debug(s"Votes: $voteList -> $guess")
    TypePrimitive(guess)
  }

  def reason(args: List[PrimitiveValue]): String = {
    val (guess, guessVotes) = voteList.maxBy( rankFn _ )
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

}