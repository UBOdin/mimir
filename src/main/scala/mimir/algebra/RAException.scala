package mimir.algebra;

class RAException(msg: String, expression: Option[Operator] = None) extends Exception(msg)
{
  override def toString(): String =
    expression match { case Some(e) => s"$msg: \n$e"; case None => msg }
}
