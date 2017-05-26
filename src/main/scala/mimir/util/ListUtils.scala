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
}