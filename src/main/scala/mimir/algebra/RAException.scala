package mimir.algebra;

class RAException(msg: String, expression: Option[Operator] = None, parent:Throwable = null) extends Exception(msg, parent)
{
  override def toString(): String =
    expression match { case Some(e) => s"$msg: \n$e"; case None => msg }

  override def getMessage(): String = 
    toString
}
