package mimir.util;

object ListUtils {
  // There's gotta be a primitive for this...
  // Temporary placeholder for now.
  def powerList[A](l: List[List[A]]): List[List[A]] =
    if(l.isEmpty){
      List(List[A]())
    } else {
      powerList[A](l.tail).flatMap( (rest) =>
        l.head.map( (head) =>
          (head :: rest)
        )
      )
    }

  def headN[A](t: Iterator[A], n: Int): List[A] =
  {
    if(n > 0){ t.next :: headN(t, n-1) }
    else { Nil }
  }

  def pivot[A](t: Seq[Seq[A]]): IndexedSeq[Seq[A]] = 
  {
    val maxWidth = t.map { _.size }.max
    (0 until maxWidth).map { 
      i => t.map { _(i) }
    }.toIndexedSeq
  }
  
  def listToTuple[A <: Object](list:List[A]):Product = {
    val size = Math.min(list.size, 22) // elements after 22 are dropped
    val classs = Class.forName("scala.Tuple" + size)
    classs.getConstructors.apply(0).newInstance(list:_*).asInstanceOf[Product]
  }
  def listToTuple2[A <: Object](list:List[A]):Tuple2[A,A] = {
    listToTuple(list).asInstanceOf[Tuple2[A,A]]
  }
  def listToTuple3[A <: Object](list:List[A]):Tuple3[A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple3[A,A,A]]
  }
  def listToTuple4[A <: Object](list:List[A]):Tuple4[A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple4[A,A,A,A]]
  }
  def listToTuple5[A <: Object](list:List[A]):Tuple5[A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple5[A,A,A,A,A]]
  }
  def listToTuple6[A <: Object](list:List[A]):Tuple6[A,A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple6[A,A,A,A,A,A]]
  }
  def listToTuple7[A <: Object](list:List[A]):Tuple7[A,A,A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple7[A,A,A,A,A,A,A]]
  }
  def listToTuple8[A <: Object](list:List[A]):Tuple8[A,A,A,A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple8[A,A,A,A,A,A,A,A]]
  }
  def listToTuple9[A <: Object](list:List[A]):Tuple9[A,A,A,A,A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple9[A,A,A,A,A,A,A,A,A]]
  }
  def listToTuple10[A <: Object](list:List[A]):Tuple10[A,A,A,A,A,A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple10[A,A,A,A,A,A,A,A,A,A]]
  }
  def listToTuple11[A <: Object](list:List[A]):Tuple11[A,A,A,A,A,A,A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple11[A,A,A,A,A,A,A,A,A,A,A]]
  }
  def listToTuple12[A <: Object](list:List[A]):Tuple12[A,A,A,A,A,A,A,A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple12[A,A,A,A,A,A,A,A,A,A,A,A]]
  }
  def listToTuple13[A <: Object](list:List[A]):Tuple13[A,A,A,A,A,A,A,A,A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple13[A,A,A,A,A,A,A,A,A,A,A,A,A]]
  }
  def listToTuple14[A <: Object](list:List[A]):Tuple14[A,A,A,A,A,A,A,A,A,A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple14[A,A,A,A,A,A,A,A,A,A,A,A,A,A]]
  }
  def listToTuple15[A <: Object](list:List[A]):Tuple15[A,A,A,A,A,A,A,A,A,A,A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple15[A,A,A,A,A,A,A,A,A,A,A,A,A,A,A]]
  }
  def listToTuple16[A <: Object](list:List[A]):Tuple16[A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple16[A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A]]
  }
  def listToTuple17[A <: Object](list:List[A]):Tuple17[A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple17[A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A]]
  }
  def listToTuple18[A <: Object](list:List[A]):Tuple18[A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple18[A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A]]
  }
  def listToTuple19[A <: Object](list:List[A]):Tuple19[A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple19[A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A]]
  }
  def listToTuple20[A <: Object](list:List[A]):Tuple20[A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple20[A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A]]
  }
  def listToTuple21[A <: Object](list:List[A]):Tuple21[A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple21[A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A]]
  }
  def listToTuple22[A <: Object](list:List[A]):Tuple22[A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A] = {
    listToTuple(list).asInstanceOf[Tuple22[A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A,A]]
  }
}