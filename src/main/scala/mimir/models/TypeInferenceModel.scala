package mimir.models

import scala.util.Random
import com.typesafe.scalalogging.slf4j.Logger

import mimir.Database
import mimir.algebra._
import mimir.util._
import org.apache.spark.sql.{DataFrame, Row, Encoders, Encoder,  Dataset}
import mimir.ml.spark.SparkML
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

object TypeInferenceModel
{
  val logger = Logger(org.slf4j.LoggerFactory.getLogger("mimir.models.TypeInferenceModel"))

  var sampleLimit = 1000
  
  def priority: Type => Int =
  {
    case TUser(_)     => 20
    case TInt()       => 10
    case TBool()      => 10
    case TDate()      => 10
    case TTimestamp() => 10
    case TInterval()  => 10
    case TType()      => 10
    case TFloat()     => 5
    case TString()    => 0
    case TRowId()     => -5
    case TAny()       => -10
  }

  def detectType(v: String): Iterable[Type] = {
    Type.tests.flatMap({ case (t, regexp) =>
      regexp.findFirstMatchIn(v).map(_ => t)
    })++
    TypeRegistry.matchers.flatMap({ case (regexp, name) =>
      regexp.findFirstMatchIn(v).map(_ => TUser(name))
    })
  }
}

 
case class TIVotes(votes:Seq[Map[Int,Double]])

case class VoteList() 
    extends Aggregator[Row, Seq[Seq[(Int,Long)]], Seq[Map[Int,(Long,Double)]]] with Serializable {
  var totalVotes = 0
  def zero = Seq[Seq[(Int, Long)]]()
  def reduce(acc: Seq[Seq[(Int, Long)]], x: Row) = {
       val newacc = x.toSeq.zipWithIndex.map(field => 
       field match {
         case (null, idx) => Seq()
         case (_, idx) => {
           totalVotes = totalVotes+1
           TypeInferenceModel.detectType(x.getString(idx)).toSeq.map(el => (Type.id(el), 1L))
         }
       }  
     )
    acc match {
      case Seq() => newacc
      case _ => {
        acc.zip(newacc).map(colas => {
           (colas._1 ++ colas._2)
        })
      }
    } 
  }
  def merge(acc1: Seq[Seq[(Int, Long)]], acc2: Seq[Seq[(Int, Long)]]) = acc1 match {
      case List() => acc2
      case x => acc2 match {
        case List() => acc1
        case x => acc1.zip(acc2).map(colas => {
           (colas._1 ++ colas._2)//.groupBy(_._1).mapValues(_.map(_._2).sum).toSeq
        })
      }
    }
    
  def finish(acc: Seq[Seq[(Int, Long)]]) = acc.map(cola => cola.groupBy(_._1).mapValues(el => {
    val votesForType = el.map(_._2).sum.toLong
    (votesForType, votesForType.toDouble/totalVotes.toDouble)
    }))
  def bufferEncoder: Encoder[Seq[Seq[(Int, Long)]]] = ExpressionEncoder()
  def outputEncoder: Encoder[Seq[Map[Int,(Long,Double)]]] = ExpressionEncoder()
}


@SerialVersionUID(1002L)
class TypeInferenceModel(name: String, val columns: IndexedSeq[String], defaultFrac: Double, sparkSql:SQLContext, query:Option[DataFrame] )
  extends Model(name)
  with SourcedFeedback
  with FiniteDiscreteDomain
{
  var trainingData:Seq[Map[Int,(Long,Double)]] = query match {
    case Some(df) => train(df)
    case None => columns.map(col => Map[Int,(Long,Double)]())
  }
  
  private def train(df:DataFrame) =
  {
    import sparkSql.implicits._
    df.limit(TypeInferenceModel.sampleLimit).select(columns.map(col(_)):_*)
      .agg(new VoteList().toColumn)
      .head()
      .asInstanceOf[Row].toSeq(0).asInstanceOf[Seq[Map[Int,Row]]]
      .map(el => el.map(sel => (sel._1 -> (sel._2.getLong(0), sel._2.getDouble(1)))))
  }

  final def learn(idx: Int, v: String):Unit =
  {
    val newtypes = TypeInferenceModel.detectType(v).toSeq.map(tp => (Type.id(tp), 1L))
    val oldTypes =  trainingData(idx).toSeq.map(el => (el._1, el._2._1))
    trainingData = trainingData.zipWithIndex.map( votesidx => if(votesidx._2 == idx) (newtypes ++ oldTypes).groupBy(_._1).mapValues(el => {
      val totalVotes = (newtypes.length + oldTypes.length).toLong
      val votesForType = el.map(_._2).sum.toLong
      (votesForType, votesForType.toDouble/totalVotes.toDouble)
    }) else votesidx._1)
  }

  def voteList(idx:Int) =  (Type.id(TString()) -> ((defaultFrac * totalVotes(idx)).toLong, defaultFrac)) :: (trainingData(idx).map(votedType => (votedType._1 -> (votedType._2._1, votedType._2._2)))).toList 
    
  def totalVotes(idx:Int) = trainingData(idx).map(votedType => votedType._2._1).sum
     
  private final def rankFn(x:(Type, Double)) =
    (x._2, TypeInferenceModel.priority(x._1) )

  def varType(idx: Int, argTypes: Seq[Type]) = TType()
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue = {
    val column = args(0).asInt
    TypePrimitive(
      Type.toSQLiteType(RandUtils.pickFromWeightedList(randomness, voteList(column).map(el => (el._1, el._2._1.toDouble)).toSeq))
    )
  }

  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue = 
  {
    val column = args(0).asInt
    getFeedback(idx, args) match {
      case None => {
        val guess =  voteList(column).map(tp => (Type.toSQLiteType(tp._1), tp._2._2)).maxBy( rankFn _ )._1
        TypePrimitive(guess)
      }
      case Some(s) => Cast(TType(), s)
    }
  }

  def validateChoice(idx: Int, v: PrimitiveValue): Boolean =
    try { Cast(TType(), v); true } catch { case _:RAException => false }

  def reason(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): String = {
    val column = args(0).asInt
    getFeedback(idx, args) match {
      case None => {
        //val (guess, guessVotes) = voteList(column).map(tp => (Type.toSQLiteType(tp._1), tp._2)).maxBy( rankFn _ )
        //val defaultPct = (defaultFrac * 100).toInt
        //val guessPct = ((guessVotes / totalVotes(column).toDouble)*100).toInt
        val (guess, guessFrac) = voteList(column).map(tp => (Type.toSQLiteType(tp._1), tp._2._2)).maxBy( rankFn _ )
        val defaultPct = (defaultFrac * 100).toInt
        val guessPct = (guessFrac*100).toInt
        val typeStr = Type.toString(guess).toUpperCase
        val reason =
          guess match {
            case TString() =>
              s"not more than $defaultPct% of the data fit anything else"
            case _ if (guessPct >= 100) =>
              "all of the data fit"
            case _ => 
              s"around $guessPct% of the data fit"
          }
        s"I guessed that $name.${columns(column)} was of type $typeStr because $reason"
      }
      case Some(t) =>
        val typeStr = Cast(TType(), t).toString.toUpperCase
        s"${getReasonWho(column,args)} told me that $name.${columns(column)} was of type $typeStr"
    }
  }

  def getDomain(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): Seq[(PrimitiveValue,Double)] = 
  {
    val column = args(0).asInt
    trainingData(idx).map( x => (TypePrimitive(Type.toSQLiteType(x._1)), x._2._2)).toSeq ++ Seq( (TypePrimitive(TString()), defaultFrac) )
  }

  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit =
  {
    if(v.isInstanceOf[TypePrimitive]){
      setFeedback(idx, args, v)
    } else {
      throw new ModelException(s"Invalid choice for $name: $v")
    }
  }

  def isAcknowledged(idx: Int,args: Seq[mimir.algebra.PrimitiveValue]): Boolean =
    isPerfectGuess(args(0).asInt) || (getFeedback(idx, args) != None)
  def isPerfectGuess(column: Int): Boolean =
    voteList(column).map( _._2._1 ).max >= totalVotes(column).toDouble
  def getFeedbackKey(idx: Int, args: Seq[PrimitiveValue]): String = 
    args(0).asString
  def argTypes(idx: Int): Seq[Type] = 
    Seq(TInt())
  def hintTypes(idx: Int): Seq[Type] = 
    Seq()


  def confidence (idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]) : Double = {
    val column = args(0).asInt
    getFeedback(idx, args) match {
      case None => {
        val (guess, guessFrac) = voteList(column).map(tp => (Type.toSQLiteType(tp._1), tp._2._2)).maxBy( rankFn _ )
        val defaultPct = (defaultFrac * 100).toInt
        val guessPct = (guessFrac*100).toInt
        val typeStr = Type.toString(guess).toUpperCase
        if (guessPct > defaultPct)
          guessFrac
        else
          defaultFrac
        }
      case Some(t) => 1.0
    }
  }
  
}
