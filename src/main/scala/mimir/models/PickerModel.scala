package mimir.models;

import scala.util.Random

import mimir.algebra._
import mimir.util._
import mimir.ctables.VGTerm
import mimir.Database

/**
 * A model representing a key-repair choice.
 * 
 * The index is ignored.
 * The one argument is a value for the key.  
 * The return value is an integer identifying the ordinal position of the selected value, starting with 0.
 */
@SerialVersionUID(1000L)
class PickerModel(override val name: String, resultColumn:String, pickFromCols:Seq[String], colTypes:Seq[Type], var source: Operator) 
  extends Model(name) 
  with Serializable
  with NeedsReconnectToDatabase
  with FiniteDiscreteDomain
{
  
  val feedback = scala.collection.mutable.Map[String,PrimitiveValue]()
  
  @transient var db: Database = null
  
  
  def argTypes(idx: Int) = {
      Seq(TRowId())
  }
  def varType(idx: Int, args: Seq[Type]) = colTypes(idx)
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]  ) = {
    val rowid = RowIdPrimitive(args(0).asString)
    feedback.get(rowid.asString) match {
      case Some(v) => v
      case None => {
        hints(0)
      }
      
    }
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = {
    val rowid = RowIdPrimitive(args(0).asString)
    this.db = db
    val iterator = db.query(Select(Comparison(Cmp.Eq, RowIdVar(), rowid), source))
    val sampleChoices = scala.collection.mutable.Map[String,Seq[PrimitiveValue]]()
    val pickCols = iterator.schema.map(_._1).zipWithIndex.flatMap( si => {
      if(pickFromCols.contains(si._1))
        Some((pickFromCols.indexOf(si._1), si))
      else
         None
    }).sortBy(_._1).unzip._2
   iterator.open()
    while(iterator.getNext() ) {
      sampleChoices(iterator.provenanceToken().asString) = pickCols.map(si => {
        iterator(si._2)
      })
    }
    iterator.close()
    
    RandUtils.pickFromList(randomness, sampleChoices(rowid.asString))
  }
  def reason(idx: Int, args: Seq[PrimitiveValue],hints: Seq[PrimitiveValue]): String = {
    val rowid = RowIdPrimitive(args(0).asString)
    feedback.get(rowid.asString) match {
      case Some(v) =>
        s"You told me that $resultColumn = $v on row $rowid"
      case None => 
         s"I used an expressions to pick a value for $resultColumn from columns: ${pickFromCols.mkString(",")}"
    }
  }
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = { 
    val rowid = args(0).asString
    feedback(rowid) = v
  }
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = {
    feedback contains(args(0).asString)
  }
  def hintTypes(idx: Int): Seq[mimir.algebra.Type] = Seq(TAny())
   
  
  def getDomain(idx: Int, args: Seq[PrimitiveValue], hints:Seq[PrimitiveValue]): Seq[(PrimitiveValue,Double)] = Seq((hints(0), 0.0))
  
  def reconnectToDatabase(db: Database) = { 
    this.db = db 
    source = db.querySerializer.desanitize(source)
  }

  /**
   * Interpose on the serialization pipeline to safely serialize the
   * source query
   */
  override def serialize: (Array[Byte], String) =
  {
    source = db.querySerializer.sanitize(source)
    val ret = super.serialize()
    return ret
  }
  
}