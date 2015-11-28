package mimir.algebra;

object ExpressionUtils {

	def getColumns(e: Expression): Set[String] = {
		e match {
			case Var(id) => Set(id)
			case _ => e.children.
						map(getColumns(_)).
						foldLeft(Set[String]())(_++_)
		}
	}


}