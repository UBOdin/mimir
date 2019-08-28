package mimir.lenses.mono

import java.sql.SQLException

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Row, Encoders, Encoder,  Dataset}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import play.api.libs.json._

import sparsity.Name

import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.exec.mode.UnannotatedBestGuess
import mimir.util.NameLookup
import mimir.serialization.AlgebraJson._

object CastColumnsLens 
{
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



object CastColumnsVoteList
    extends Aggregator[Row,  Seq[(Long,Seq[(Int,Long)])], Seq[(Long,Seq[(Type,Double)])]] with Serializable 
{
  def zero = Seq[(Long,Seq[(Int, Long)])]()
  def reduce(acc: Seq[(Long, Seq[(Int, Long)])], x: Row) = {
     val newacc = x.toSeq.zipWithIndex.map(field => 
       field match {
         case (null, idx) => (0L, Seq[(Int, Long)]())
         case (_, idx) => {
           if(!x.isNullAt(idx)){
             val cellVal = x.getString(idx)
             (1L, CastColumnsLens.detectType(cellVal).toSeq.map(el => (Type.toIndex(el), 1L)))
           }
           else (0L, Seq[(Int, Long)]())
         }
       }  
     )
    merge(acc, newacc)
  }
  def reduce(acc: Seq[(Long, Seq[(Int, Long)])], x: Seq[Seq[Type]]) =
  {
    merge(acc, 
      x.map {  
        case Seq() => (0L, Seq[(Int, Long)]())
        case field => (1L, field.map { el => (Type.toIndex(el), 1L) })
      }
    )
  }
  def merge(acc1: Seq[(Long, Seq[(Int, Long)])], acc2: Seq[(Long, Seq[(Int, Long)])]) = acc1 match {
      case Seq() | Seq( (0L, Seq()) ) => acc2
      case x => acc2 match {
        case Seq() | Seq( (0L, Seq()) ) => acc1
        case x => {
          acc1.zip(acc2).map(oldNew => {
            (
              oldNew._1._1+oldNew._2._1, 
              (oldNew._1._2++oldNew._2._2)
                .groupBy { _._1 }
                .mapValues { _.map { _._2 }.sum }
                .toSeq
            )
          })
        }
      }
    }
    
  def finish(acc: Seq[(Long, Seq[(Int, Long)])]) = 
    acc.map { case (totalCount, countByType) =>
      (
        totalCount, 
        countByType.groupBy { _._1 }
                   .map { case (typeIndex, counts) => 
                      Type.fromIndex(typeIndex) ->
                        counts.map { _._2 }.sum.toDouble / totalCount
                   }
                   .toSeq
                   .sortBy { -_._2 }
      )
    }
  def bufferEncoder: Encoder[Seq[(Long, Seq[(Int, Long)])]] = ExpressionEncoder()
  def outputEncoder: Encoder[Seq[(Long, Seq[(Type,Double)])]] = ExpressionEncoder()
}