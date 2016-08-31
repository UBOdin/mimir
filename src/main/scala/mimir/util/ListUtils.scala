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
}