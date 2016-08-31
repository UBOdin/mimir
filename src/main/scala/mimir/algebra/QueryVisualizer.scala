package mimir.algebra

import mimir.web.OperatorNode
import mimir.ctables.CTables

object QueryVisualizer {
  def convertToTree(op: Operator): OperatorNode = {
    op match {
      case Project(cols, source) => {
        val projArg = cols.filter { case ProjectArg(col, exp) => CTables.isProbabilistic(exp) }
        if(projArg.isEmpty)
          convertToTree(source)
        else {
          var params = 
            projArg.flatMap( projectArg => CTables.getVGTerms(projectArg.expression) ).
                    map(_.model._1) 
          if(params.isEmpty) {
            convertToTree(source)
          } else {
            new OperatorNode(params.head, List(convertToTree(source)), Some(params.head))
          }
        }
      }
      case Join(lhs, rhs) => new OperatorNode("Join", List(convertToTree(lhs), convertToTree(rhs)), None)
      case Union(lhs, rhs) => new OperatorNode("Union", List(convertToTree(lhs), convertToTree(rhs)), None)
      case Table(name, schema, metadata) => new OperatorNode(name, List[OperatorNode](), None)
      case o: Operator => new OperatorNode(o.getClass.toString, o.children.map(convertToTree(_)), None)
    }
  }
}