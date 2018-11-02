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

  def detectType(vo: Option[String]): Iterable[Type] = {
    vo match {
      case Some(v) => {
        Type.tests.flatMap({ case (t, regexp) =>
          regexp.findFirstMatchIn(v).map(_ => t)
        })++
        TypeRegistry.matchers.flatMap({ case (regexp, name) =>
          regexp.findFirstMatchIn(v).map(_ => TUser(name))
        }) match {
          case Seq() => Seq(TAny())
          case x => x
        }
      }
      case None => Seq()
    }
  }
}

case class TIVotes(votes:Seq[Seq[String]])

case class VoteList(colIdx: Int) 
    extends Aggregator[Row, Seq[String], Map[String,Double]] with Serializable {
  def zero = Seq[String]()
  def reduce(acc: Seq[String], x: Row) = acc ++ x.getSeq(0)(colIdx)
  def merge(acc1: Seq[String], acc2: Seq[String]) = acc1 ++ acc2
  def finish(acc: Seq[String]) = acc.groupBy(identity).mapValues(_.size)
  def bufferEncoder: Encoder[Seq[String]] = ExpressionEncoder()
  def outputEncoder: Encoder[Map[String,Double]] = ExpressionEncoder()
}

case class VoteCount(colIdx: Int) 
    extends Aggregator[Row, Long, Long] with Serializable {
  def zero = 0
  def reduce(acc: Long, x: Row) = acc + (x.getSeq(0)(colIdx) match {
    case Seq() => 0
    case _ => 1
  })
  def merge(acc1: Long, acc2: Long) = acc1 + acc2
  def finish(acc: Long) = acc
  def bufferEncoder: Encoder[Long] = Encoders.scalaLong
  def outputEncoder: Encoder[Long] = Encoders.scalaLong
}

@SerialVersionUID(1002L)
class TypeInferenceModel(name: String, val columns: IndexedSeq[String], defaultFrac: Double, sparkSql:SQLContext, query:Option[DataFrame] )
  extends Model(name)
  with SourcedFeedback
  with FiniteDiscreteDomain
{
  
  
  var trainingData:Dataset[TIVotes] = query match {
    case Some(df) => train(df)
    case None => {
      sparkSql.createDataset(
       List(TIVotes(columns.map(col => Seq()))))(Encoders.product[TIVotes])
    }
  }

  private def train(df:DataFrame) =
  {
    import sparkSql.implicits._
    df.limit(TypeInferenceModel.sampleLimit).select(columns.map(col(_)):_*).map(row => {
      TIVotes(row.schema.fields.zipWithIndex.map(se => TypeInferenceModel.detectType(
         if(row.isNullAt(se._2)) None else Some(s"${row(se._2)}")
       ).toSeq.map(_.toString())))
     })(Encoders.product[TIVotes])
  }

  final def learn(idx: Int, v: String):Unit =
  {
    trainingData = trainingData.union(sparkSql.createDataset(
       List(TIVotes(columns.zipWithIndex.map(col => (col._2 match {
         case `idx` => TypeInferenceModel.detectType(Some(v)).map(_.toString())
         case _ => Seq()
       }).toSeq).toSeq)))(Encoders.product[TIVotes]))
  }

  def voteList(idx:Int) =  (TString() -> defaultFrac * totalVotes(idx)) :: (trainingData.agg(new VoteList(idx).toColumn).head().getMap[String,Double](0)
      .flatMap(ts => {
        Type.fromString(ts._1) match {
          case TAny() => None
          case x => Some((x, ts._2))
        }
        })).toList
    
  
  def totalVotes(idx:Int) = trainingData.agg(new VoteCount(idx).toColumn).limit(1).collect().map(_.getLong(0)).headOption.getOrElse(0L)
    
  
  private final def rankFn(x:(Type, Double)) =
    (x._2, TypeInferenceModel.priority(x._1) )

  def varType(idx: Int, argTypes: Seq[Type]) = TType()
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue = {
    val column = args(0).asInt
    TypePrimitive(
      RandUtils.pickFromWeightedList(randomness, voteList(column).toSeq)
    )
  }

  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue = 
  {
    val column = args(0).asInt
    getFeedback(idx, args) match {
      case None => {
        val guess =  voteList(column).maxBy( rankFn _ )._1
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
        val (guess, guessVotes) = voteList(column).maxBy( rankFn _ )
        val defaultPct = (defaultFrac * 100).toInt
        val guessPct = ((guessVotes / totalVotes(column).toDouble)*100).toInt
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
    trainingData.agg(new VoteList(column).toColumn).head().getMap[String, Double](0).toSeq.map( x => (TypePrimitive(Type.fromString(x._1)), x._2)) ++ Seq( (TypePrimitive(TString()), defaultFrac) )
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
    voteList(column).map( _._2 ).max >= totalVotes(column).toDouble
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
        val (guess, guessVotes) = voteList(column).maxBy( rankFn _ )
        val defaultPct = (defaultFrac * 100).toInt
        val guessPct = ((guessVotes / totalVotes(column).toDouble)*100).toInt
        val typeStr = Type.toString(guess).toUpperCase
        if (guessPct > defaultPct)
          guessVotes / totalVotes(column).toDouble
        else
          defaultFrac
        }
      case Some(t) => 1.0
    }
  }
  
}
