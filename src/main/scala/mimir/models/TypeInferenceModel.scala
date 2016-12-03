package mimir.models

import scala.util.Random
import scala.collection.mutable.ListBuffer
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
    })++TypeRegistry.typeList.foreach((userTypeDef)=>{
      if(v.matches(userTypeDef._2)) {
        TUser(userTypeDef._1, userTypeDef._2, userTypeDef._3)
      } else { None }
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

/*
Adding new UserTypes to mimir (TUser Types)
- All new types must be added the TypeList in object TypeList to effect the system
- Parameterized types are now supported. The current parameter is Tuple(name:String, regex:String, sqlType:Type), remember to update Tuple# to the right number in object TypeList
  - name:String is the name of the new type, must be unique
  - regex:String is the regular expression that this new type should match on, if the column is of this type will be determined by the threshold, consider the basic types when making this
  - sqlType:Type this is the underlying type of the new type, example TFloat, TDouble... This is for potential speed up later and to better capture the output

Extending the UserType
- This is if you want to add new functionality to TUser, such as adding a new parameter
These are the files that need to change to extend the TUser
  - mimir.lenses.TypeInference:
    - In class TypeInferenceTypes add the new parameters to TUser
    - In class TypeInferer add the new parameters to TUser
  - mimir.utils.JDBCUtils:
    - changes here are made in the function convert, in the case class TUser. This only needs
  - mimir.algebra.Expression:
    - change TUser parameters to match the new desired parameters
    - change sealed trait Type so that all the instances of TUser match the new parameters
  - mimir.sql.sqlite.SQLiteCompat:
    - update TUser type parameters
 */
object TypeList{
  val typeList = ListBuffer[(String,String,Type)]()

  typeList += ("TUser",         "USER",                            TString())
  typeList += ("TWeight",       "KG*",                             TString())
  typeList += ("FireCompany",   "^[a-zA-Z]\\d{3}$",                TString())
  typeList += ("ZipCode",       "^\\d{5}(?:[-\\s]\\d{4})?$",       TInt())
  typeList += ("Container",     "[A-Z]{4}[0-9]{7}",                TString())
  typeList += ("CarrierCode",   "[A-Z]{4}",                        TString())
  typeList += ("MMSI",          "MID\\d{6}|0MID\\d{5}|00MID\\{4}", TString())
  typeList += ("BillOfLanding", "[A-Z]{8}[0-9]{8}",                TString())
}
