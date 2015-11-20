package mimir.algebra;

object ExpressionUtils {

	def getColumns(e: Expression): Set[String] = {
		e match {
			case Var(id) => Set(id)
			_ => e.children.map(varsOfExpression(_)).foldLeft(Set[String]())(_++_)
		}
	}

}