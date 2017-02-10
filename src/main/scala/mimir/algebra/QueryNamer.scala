package mimir.algebra;

object QueryNamer 
{
	def nameQuery(op: Operator): String = 
	{
		op match { 
			case Table(name, alias, _, _) => name
			case Project(cols, src) => 
				cols.length match {
					case 1 => cols(0).name+"_from_"+nameQuery(src)
					case 2 => 
						cols(0).name+"_and_"+
						cols(1).name+"_from_"+
						nameQuery(src)
					case _ => nameQuery(src)
				}
			case Select(cond, src) =>
				nameQuery(src)+"_where_"+nameBoolExpression(cond)
			case Join(lhs, rhs) =>
				if(hasJoinOrUnion(lhs) || hasJoinOrUnion(rhs)) {
					(getRelationNames(lhs)++getRelationNames(rhs)).mkString("_")
				} else {
					nameQuery(lhs)+"_"+nameQuery(rhs)
				}
			case Union(lhs, rhs) =>
				if(hasJoinOrUnion(lhs) || hasJoinOrUnion(rhs)) {
					(getRelationNames(lhs)++getRelationNames(rhs)).mkString("_plus_")
				} else {
					nameQuery(lhs)+"_plus_"+nameQuery(rhs)
				}
		}
	}

	def nameArithExpression(e: Expression): String =
	{
		"an_expression"
	}

	def nameBoolExpression(e: Expression): String =
	{
		"a_condition_holds"
	}

	def hasJoinOrUnion(q: Operator): Boolean =
	{
		q match { 
			case Join(_,_) => true
			case Union(_,_) => true
			case _ => q.children.map( hasJoinOrUnion(_) ).foldLeft(false)( _ || _ )
		}
	}
	def getRelationNames(q: Operator): List[String] =
	{
		q match {
			case Table(tn, ta, _, _) => List(tn)
			case _ => q.children.map( getRelationNames(_) ).
														foldLeft(List[String]())( _ ++ _ )
		}
	}


}