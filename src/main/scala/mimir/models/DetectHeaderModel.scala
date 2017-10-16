package mimir.models

import scala.util.Random
import com.typesafe.scalalogging.slf4j.Logger

import scala.collection.mutable.ListBuffer

import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.models._
import mimir.views._


@SerialVersionUID(1001L)
class DetectHeaderModel(override val name: String, var view_name: String)
extends Model(name)
with Serializable
{
  def detect_header(db: Database, query: Operator): Unit = {
    var detect = false;
    var header : Seq[mimir.algebra.PrimitiveValue] = null;
    var arrs : Seq[Seq[mimir.algebra.PrimitiveValue]] = Seq.empty[Seq[mimir.algebra.PrimitiveValue]];
    db.query(Limit(0,Some(1),query))(_.foreach{result =>
      header =  result.tuple
    })
    db.query(Limit(1,Some(5),query))(_.foreach{result =>
      arrs:+= result.tuple
    })
    val sample  = arrs.iterator
    val columnLength = header.size;
    var columnType =  scala.collection.mutable.Map[Int, String]()
    for(i <- 0 until columnLength ){
      columnType+= (i -> null)
    }

    var flag = 0;
    while(sample.hasNext){
      flag = 1;
      val row = sample.next

      for (col <- columnType.keySet){
        if(row(col) != NullPrimitive()){
          var i  = Cast.apply(TFloat(),row(col))
          if(i==NullPrimitive()){
            i = Cast.apply(TDate(),row(col))
          }
          if(i != NullPrimitive()){
            if(columnType(col) != "true"){
              if(columnType(col) == null){
                columnType(col) = "true"
              }
              else{
                columnType -= col
              }
            }
          }
          else{
              columnType(col)  = (row(col).toString().length()-2).toString();
          }
        }
      }
    }
    if (flag == 0){
      detect =  false;
    }
    var hasHeader = 0
    for (c<-columnType.keySet){
      if(columnType(c)!="true"){
        if (header(c).toString.length()-2 == columnType(c).toInt) {
            hasHeader=hasHeader-1;
        } else {
            hasHeader=hasHeader+1;
        }
      }else {
          var i  = Cast.apply(TFloat(),header(c))
          if(i==NullPrimitive()){
            i = Cast.apply(TDate(),header(c))
          }
          if( i == NullPrimitive()){
            hasHeader = hasHeader+1
          }else{
            hasHeader=hasHeader - 1;
            detect =  false;
          }
        }
      }
    detect = hasHeader > 0
    if(detect ==true){
      view_name = name+"_HEADER"
    }else{
      view_name = name+"_HEADER_FORCED"
    }
  }

  val feedback = scala.collection.mutable.Map[String,PrimitiveValue]()

  def argTypes(idx: Int) = {
      Seq(TRowId())
  }
  def varType(idx: Int, args: Seq[Type]) = TType()
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ) = {
    //println(s"MissingKeyModel:bestGuess: idx: $idx args: ${args.mkString("[ ",","," ]")} hints: ${hints.mkString("[ ",","," ]")}")
    val rowid = RowIdPrimitive(args(0).asString)
    feedback.get(rowid.asString+"_"+idx) match {
      case Some(v) => v
      case None => hints(0)
    }
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = {
    hints(0)
  }
  def reason(idx: Int, args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): String = {
    val rowid = RowIdPrimitive(args(0).asString)
    feedback.get(rowid.asString+"_"+idx) match {
      case Some(v) => v match {
          case NullPrimitive() => {
            "You told me that the row of this cell was missing and that the value of this cell is unknown so I have made it NULL."
          }
          case i => {
            s"You told me that this key was missing because it was in a sequence but not in the query results: $i"
          }
      }
      case None => hints(0) match {
        case NullPrimitive() => {
          "I guessed that the row of this cell was missing. The value of this cell is unknown so I have made it NULL."
        }
        case i => {
          s"I guessed that this key was missing because it was in a sequence but not in the query results: $i"
        }
      }
    }
  }
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = {
    val rowid = args(0).asString
    feedback(rowid+"_"+idx) = v
  }
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = {
    feedback contains(args(0).asString+"_"+idx)
  }
  def hintTypes(idx: Int): Seq[mimir.algebra.Type] = Seq(TAny())


}
