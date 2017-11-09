package mimir.algebra;

object QueryNamer 
{
	def apply(op: Operator) = nameQuery(op)

	def nameQuery(op: Operator): String = 
	{
		op match { 
			case Table(name,alias, _, _) => name
			case View(name, _, _) => name
			case AdaptiveView(model, name, _, _) => (model+"_"+name)
			case Project(cols, src) => 
				cols.length match {
					case 1 => cols(0).name+"_FROM_"+nameQuery(src)
					case 2 => 
						cols(0).name+"_AND_"+
						cols(1).name+"_FROM_"+
						nameQuery(src)
					case _ => nameQuery(src)
				}
			case Select(cond, src) =>
				nameQuery(src)+"_WHERE_"+nameBoolExpression(cond)
			case Join(lhs, rhs) =>
				if(hasJoinOrUnion(lhs) || hasJoinOrUnion(rhs)) {
					(getRelationNames(lhs)++getRelationNames(rhs)).mkString("_")
				} else {
					nameQuery(lhs)+"_"+nameQuery(rhs)
				}
			case LeftOuterJoin(lhs, _, _) =>
				nameQuery(lhs)
			case Union(lhs, rhs) =>
				if(hasJoinOrUnion(lhs) || hasJoinOrUnion(rhs)) {
					(getRelationNames(lhs)++getRelationNames(rhs)).mkString("_PLUS_")
				} else {
					nameQuery(lhs)+"_PLUS_"+nameQuery(rhs)
				}
			case Aggregate(_, _, src) =>
				nameQuery(src)+"_SUMMARIZED"
			case Limit(_, _, src) =>
				nameQuery(src)
			case Sort(cols, src) =>
				nameQuery(src)+"_BY_"+cols.flatMap(col => ExpressionUtils.getColumns(col.expression)).mkString("_")
			case EmptyTable(_) => 
				"EMPTY_QUERY"
			case SingletonTable(_) => 
				"SINGLETON"
			case Annotate(src, _) => nameQuery(src)
			case Recover(src, _) => nameQuery(src)
			case ProvenanceOf(src) => nameQuery(src)
		}
	}

	def nameArithExpression(e: Expression): String =
	{
		"AN_EXPRESSION"
	}

	def nameBoolExpression(e: Expression): String =
	{
		"A_CONDITION_HOLDS"
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
