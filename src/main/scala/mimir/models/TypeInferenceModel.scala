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
    ("(\\+|-)?([0-9]+)",               TInt()),
    ("(\\+|-)?([0-9]*(\\.[0-9]+)?)",   TFloat()),
    ("[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}", TDate()),
    ("(?i:true|false)",                TBool())
  )

  def priority: Type => Int =
  {
    case TUser(_,_,_) => 20
    case TInt()       => 10
    case TBool()      => 10
    case TDate()      => 10
    case TType()      => 10
    case TFloat()     => 5
    case TString()    => 0
    case TRowId()     => -5
    case TAny()       => -10
  }

  def detectType(v: String): List[Type] = {
    typeTests.flatMap({ case (test, t) =>
      if(v.matches(test)){ Some(t) }
      else { None }
    })++TypeRegistry.typeList.flatMap((userTypeDef)=>{
      if(v.matches(userTypeDef._2)) {
        Some(TUser(userTypeDef._1, userTypeDef._2, userTypeDef._3))
      } else { None }
    })

  }
}

@SerialVersionUID(1000L)
class TypeInferenceModel(name: String, column: String, defaultFrac: Double)
  extends SingleVarModel(name)
{
  var totalVotes = 0.0
  val votes = scala.collection.mutable.Map[Type, Double]()

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
    (TString(), defaultFrac * totalVotes) :: votes.toList

  private final def rankFn(x:(Type, Double)) =
    (x._2, TypeInferenceModel.priority(x._1) )

  def varType(argTypes: List[Type]) = TType()
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
        case TString() =>
          s"not more than $defaultPct% of the data fit anything else"
        case _ => 
          s"around $guessPct% of the data matched"
      }

    s"I guessed that $column was of type $typeStr because $reason"
  }

}