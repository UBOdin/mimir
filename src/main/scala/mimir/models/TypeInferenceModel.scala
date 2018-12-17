package mimir.models

import scala.util.Random
import com.typesafe.scalalogging.slf4j.Logger

import mimir.Database
import mimir.algebra._
import mimir.algebra.typeregistry.TypeRegistry
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

}

 
case class TIVotes(votes:Seq[Map[Int,Double]])

case class VoteList(types:(TypeRegistry with Serializable)) 
    extends Aggregator[Row,  Seq[(Long,Seq[(Int,Long)])], Seq[(Long,Map[Int,(Long,Double)])]] with Serializable {
  def zero = Seq[(Long,Seq[(Int, Long)])]()
  def reduce(acc: Seq[(Long, Seq[(Int, Long)])], x: Row) = {
     val newacc:Seq[(Long, Seq[(Int, Long)])] = 
      x.toSeq.zipWithIndex.map(field => 
       field match {
         case (null, idx) => (0L, Seq[(Int, Long)]())
         case (_, idx) => {
           if(!x.isNullAt(idx)){
             val cellVal = x.getString(idx)
             (1L, types.testForTypes(cellVal).toSeq.map(el => (types.idForType(el):Int, 1L)))
           }
           else (0L, Seq[(Int, Long)]())
         }
       }  
     )
    acc match {
      case Seq() | Seq( (0, Seq()) ) => newacc
      case _ => {
        acc.zip(newacc).map(oldNew => {
          (oldNew._1._1+oldNew._2._1, oldNew._1._2++oldNew._2._2)
        })
      }
    } 
  }
  def merge(acc1: Seq[(Long, Seq[(Int, Long)])], acc2: Seq[(Long, Seq[(Int, Long)])]) = acc1 match {
      case Seq() | Seq(0 Seq()) => acc2
      case x => acc2 match {
        case Seq() | Seq(0 Seq()) => acc1
        case x => {
          acc1.zip(acc2).map(oldNew => {
            (oldNew._1._1+oldNew._2._1, oldNew._1._2++oldNew._2._2)
          })
        }
      }
    }
    
  def finish(acc: Seq[(Long, Seq[(Int, Long)])]) = 
    acc.map(cola => (cola._1, cola._2.groupBy(_._1).mapValues(el => {
      val votesForType = el.map(_._2).sum.toLong
      (votesForType, votesForType.toDouble/cola._1)
    })))
  def bufferEncoder: Encoder[Seq[(Long, Seq[(Int, Long)])]] = ExpressionEncoder()
  def outputEncoder: Encoder[Seq[(Long, Map[Int,(Long,Double)])]] = ExpressionEncoder()
}


@SerialVersionUID(1002L)
class TypeInferenceModel(name: String, val columns: IndexedSeq[String], defaultFrac: Double, sparkSql:SQLContext, query:Option[DataFrame], types:(TypeRegistry with Serializable))
  extends Model(name)
  with SourcedFeedback
  with FiniteDiscreteDomain
{
  // Sequence of 
  //   Total # of Votes
  //   Map from TypeID -> (# of Votes for Type, % of Votes for Type)
  // One sequence element per input column
  var trainingData:Seq[(Long, Map[Int,(Long,Double)])] = 
    query match {
      case Some(df) => train(df)
      case None => columns.map(col => (0L,Map[Int,(Long,Double)]()))
    }
  
  private def train(df:DataFrame) =
  {
    import sparkSql.implicits._
    df.limit(TypeInferenceModel.sampleLimit).select(columns.map(col(_)):_*)
      .agg(new VoteList(types).toColumn)
      .head()
      .asInstanceOf[Row].toSeq(0).asInstanceOf[Seq[Row]]
      .map(el => (el.getLong(0), el.getMap[Int,Row](1).toMap) )
      .map(el => (el._1, el._2.map(sel => (sel._1 -> (sel._2.getLong(0), sel._2.getDouble(1))))))
  }

  final def learn(idx: Int, v: String):Unit =
  {
    val newtypes = types.testForTypes(v).toSeq.map(tp => (types.idForType(tp):Int, 1L))
    val oldAcc = trainingData(idx)
    val (oldTotal, oldTypes) =  (oldAcc._1, oldAcc._2.toSeq.map(el => (el._1, el._2._1)))
    val newTotalVotes = (1+ oldTotal).toLong
    trainingData = 
      trainingData.zipWithIndex.map { votesidx => 
        if(votesidx._2 == idx) {
          ( newTotalVotes, 
            (newtypes ++ oldTypes).groupBy { _._1:Int }
                                  .mapValues(el => {
                                      val votesForType = el.map(_._2).sum.toLong
                                      (votesForType, votesForType.toDouble/newTotalVotes.toDouble)
                                  })
                                  .toMap[Int,(Long,Double)]
          )
        } else { votesidx._1 }
      }
  }

  def voteList(idx:Int): Seq[(Integer, (Long,Double))] =
    Seq( 
      ( (types.idForType(TString())) -> 
          ((defaultFrac * totalVotes(idx)).toLong, defaultFrac)
      )
    ) ++ (
      trainingData(idx)._2.map { votedType => 
        ((votedType._1:Integer) -> (votedType._2._1, votedType._2._2))
      }.toSeq
    )
    
  def totalVotes(idx:Int) = trainingData(idx)._1
     
  private final def rankFn(x:(Type, Double)) =
    (x._2, TypeInferenceModel.priority(x._1) )

  def varType(idx: Int, argTypes: Seq[BaseType]) = TType()
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue = {
    val column = args(0).asInt
    TypePrimitive(
      types.typeForId(
        RandUtils.pickFromWeightedList(
          randomness, 
          voteList(column)
            .map{ el => (el._1, el._2._1.toDouble) } 
            .toSeq
        )
      )
    )
  }

  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue = 
  {
    val column = args(0).asInt
    getFeedback(idx, args) match {
      case None => {
        val guess =  voteList(column).map(tp => (types.typeForId(tp._1), tp._2._2)).maxBy( rankFn _ )._1
        //println(s"bestGuess(idx: $idx, args: ${args.mkString(",")}, hints:${hints.mkString(",")}) => $guess")
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
        val (guess, guessFrac) = voteList(column).map(tp => (types.typeForId(tp._1), tp._2._2)).maxBy( rankFn _ )
        val defaultPct = (defaultFrac * 100).toInt
        val guessPct = (guessFrac*100).toInt
        val typeStr = guess.toString.toUpperCase
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
    trainingData(idx)._2.map( x => (TypePrimitive(types.typeForId(x._1)), x._2._2)).toSeq ++ Seq( (TypePrimitive(TString()), defaultFrac) )
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
  def argTypes(idx: Int): Seq[BaseType] = 
    Seq(TInt())
  def hintTypes(idx: Int): Seq[BaseType] = 
    Seq()


  def confidence (idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]) : Double = {
    val column = args(0).asInt
    getFeedback(idx, args) match {
      case None => {
        val (guess, guessFrac) = voteList(column).map(tp => (types.typeForId(tp._1), tp._2._2)).maxBy( rankFn _ )
        val defaultPct = (defaultFrac * 100).toInt
        val guessPct = (guessFrac*100).toInt
        if (guessPct > defaultPct)
          guessFrac
        else
          defaultFrac
        }
      case Some(t) => 1.0
    }
  }
  
}
