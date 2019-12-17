package mimir.exec.result

import java.sql.SQLException
import mimir.algebra._
import mimir.provenance._
import mimir.ctables._


sealed trait Row
{
  def tuple: Seq[PrimitiveValue]
  def tupleMap: Map[ID,PrimitiveValue] =
    tupleSchema.zip(tuple).map { x => (x._1._1 -> x._2) }.toMap

  def apply(idx: Int): PrimitiveValue
  def apply(name: ID): PrimitiveValue = {
    val idx = tupleSchema.indexWhere( _._1.equals(name) )
    if(idx < 0){
      throw new SQLException(s"Field '$name' not in tuple: ${tupleSchema.map(_._1)}")
    } else {
      return apply(idx)
    }
  }

  def annotation(idx: Int): PrimitiveValue
  def annotation(name: ID): PrimitiveValue

  def tupleSchema: Seq[(ID,Type)]

  def provenance: RowIdPrimitive = 
    annotation( Provenance.rowidColnameBase)  match {
    case NullPrimitive() => RowIdPrimitive("")
    case x => RowIdPrimitive(x.asString)
  }
  
  def isDeterministic(): Boolean = 
    annotation(OperatorDeterminism.mimirRowDeterministicColumnName) match {
      case NullPrimitive() => false
      case BoolPrimitive(t) => t
      case IntPrimitive(i) => i match {
        case  1 => true
        case  0 => false 
        case -1 => false
        case _ => throw new RAException("Error getting determinism")
      }
      case _ => throw new RAException("Error getting determinism")
    }
  def isColDeterministic(col: ID): Boolean = 
    annotation(OperatorDeterminism.mimirColDeterministicColumn(col)) match {
      case NullPrimitive() => false
      case BoolPrimitive(t) => t
      case IntPrimitive(i) => i match {
        case  1 => true
        case  0 => false 
        case -1 => false
        case _ => throw new RAException("Error getting determinism")
      }
      case _ => throw new RAException("Error getting determinism")      
    }
  def isColDeterministic(idx: Int): Boolean =
    isColDeterministic(tupleSchema(idx)._1)

  override def toString: String = "<" + 
    tupleSchema.zip(tuple).map { case ((name,_),v) => name+":"+v }.mkString(", ") + 
  ">"
}

case class LazyRow(
  input: Row, 
  tupleDefinition: Seq[Row=>PrimitiveValue],
  annotationDefinition: Seq[Row=>PrimitiveValue],
  val tupleSchema: Seq[(ID, Type)],
  val annotationIndexes: Map[ID,Int]
) extends Row {
  def tuple: Seq[PrimitiveValue] = 
    tupleDefinition.map { _(input) }
  def apply(idx: Int): PrimitiveValue = 
  {
    try { tupleDefinition(idx)(input) }
    catch { 
      case _:IndexOutOfBoundsException => 
        throw new SQLException(s"$tuple does not have a column $idx")
      case e:Throwable => 
        throw new RuntimeException(
          s"Error Decoding ${tupleSchema(idx)._1} (${tupleSchema(idx)._2})",
          e
        )
    }
  }
  def annotation(name: ID): PrimitiveValue = 
    annotation(annotationIndexes(name))
  def annotation(idx: Int): PrimitiveValue = 
    annotationDefinition(idx)(input)
}

case class ExplicitRow(val tuple: Seq[PrimitiveValue], val annotations: Seq[PrimitiveValue], val source: ResultIterator)
  extends Row
{
  def apply(idx: Int): PrimitiveValue = tuple(idx)

  def annotation(idx: Int): PrimitiveValue = if(idx < annotations.size) { annotations(idx) } else { NullPrimitive() }
  def annotation(name: ID): PrimitiveValue = if(source.hasAnnotation(name)){ annotation(source.getAnnotationIdx(name)) } else { NullPrimitive() }

  def tupleSchema = source.schema;
}